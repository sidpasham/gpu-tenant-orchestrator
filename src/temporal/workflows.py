from datetime import timedelta
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from src.placement.scheduler import PENDING_CAPACITY_STATUS, RESERVED_STATUS
    from src.temporal.activities import (
        activate_gpu_reservation,
        plan_gpu_allocation,
        release_gpu_reservation,
        run_helm_deploy,
    )


@workflow.defn(name="GPUAllocationWorkflow")
class GPUAllocationWorkflow:
    @workflow.run
    async def run(self, data: dict) -> dict:
        placement = await workflow.execute_activity(
            plan_gpu_allocation,
            data,
            start_to_close_timeout=timedelta(minutes=1),
        )
        if placement.get("status") == PENDING_CAPACITY_STATUS:
            return {
                "status": PENDING_CAPACITY_STATUS,
                "message": placement.get("reason", "GPU capacity is unavailable."),
                "placement": placement,
            }
        if placement.get("status") != RESERVED_STATUS:
            raise ValueError("GPU placement did not return a reservation")

        deploy_input = dict(data)
        deploy_input["placement"] = placement

        try:
            result = await workflow.execute_activity(
                run_helm_deploy,
                deploy_input,
                start_to_close_timeout=timedelta(minutes=5),
            )
            await workflow.execute_activity(
                activate_gpu_reservation,
                {"reservation_id": placement["reservation_id"]},
                start_to_close_timeout=timedelta(minutes=1),
            )
        except Exception:
            await workflow.execute_activity(
                release_gpu_reservation,
                {"reservation_id": placement.get("reservation_id")},
                start_to_close_timeout=timedelta(minutes=1),
            )
            raise

        return {
            "status": "ACTIVE",
            "message": result,
            "placement": placement,
        }
