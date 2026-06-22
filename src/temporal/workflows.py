from datetime import timedelta
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from src.temporal.activities import run_helm_deploy


@workflow.defn(name="GPUAllocationWorkflow")
class GPUAllocationWorkflow:
    @workflow.run
    async def run(self, data: dict) -> str:
        result = await workflow.execute_activity(
            run_helm_deploy,
            data,
            start_to_close_timeout=timedelta(minutes=5),
        )
        return result
