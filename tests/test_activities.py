import asyncio
from types import SimpleNamespace

import pytest

from src.temporal import activities


def test_get_allocation_rules_maps_supported_tiers_case_insensitively():
    assert activities.get_allocation_rules("premium") == 2
    assert activities.get_allocation_rules("Premium") == 2
    assert activities.get_allocation_rules("standard") == 1


def test_get_allocation_rules_returns_zero_for_unsupported_input():
    assert activities.get_allocation_rules("gold") == 0
    assert activities.get_allocation_rules("") == 0
    assert activities.get_allocation_rules(None) == 0


def test_run_helm_deploy_builds_expected_command(monkeypatch):
    calls = []
    heartbeats = []
    monkeypatch.setattr(activities, "HELM_DRY_RUN", False)
    monkeypatch.setattr(activities, "HELM_NAMESPACE", "default")
    monkeypatch.setattr(activities, "HELM_NAMESPACE_TEMPLATE", "")
    monkeypatch.setattr(activities, "HELM_CREATE_NAMESPACE", False)
    monkeypatch.setattr(activities, "HELM_MOCK_GPU", True)
    monkeypatch.setattr(activities, "HELM_KUBE_APISERVER", "")
    monkeypatch.setattr(activities, "HELM_KUBE_TLS_SERVER_NAME", "")
    monkeypatch.setattr(activities, "HELM_KUBE_CA_FILE", "")
    monkeypatch.setattr(activities, "HELM_KUBE_TOKEN_FILE", "")
    monkeypatch.setattr(activities, "HELM_KUBE_INSECURE_SKIP_TLS_VERIFY", False)

    def fake_run(command, capture_output, text, timeout):
        calls.append(
            {
                "command": command,
                "capture_output": capture_output,
                "text": text,
                "timeout": timeout,
            }
        )
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(activities.subprocess, "run", fake_run)
    monkeypatch.setattr(activities.activity, "heartbeat", heartbeats.append)

    result = asyncio.run(
        activities.run_helm_deploy(
            {"customer_id": "team-a", "tier": "premium"}
        )
    )

    assert result == (
        "Successfully provisioned resources via Helm for release: tenant-team-a"
    )
    assert heartbeats == ["Initiating Helm deployment execution context"]
    assert calls == [
        {
            "command": [
                "helm",
                "upgrade",
                "--install",
                "tenant-team-a",
                "./helm/tenant-workload",
                "--set",
                "customerId=team-a",
                "--set",
                "tier=premium",
                "--set",
                "gpuCount=2",
                "--set",
                "mockGpu=true",
                "--namespace",
                "default",
            ],
            "capture_output": True,
            "text": True,
            "timeout": 300,
        }
    ]


def test_run_helm_deploy_can_render_chart_without_cluster_access(monkeypatch):
    calls = []

    def fake_run(command, capture_output, text, timeout):
        calls.append(command)
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(activities, "HELM_DRY_RUN", True)
    monkeypatch.setattr(activities, "HELM_NAMESPACE", "default")
    monkeypatch.setattr(activities, "HELM_NAMESPACE_TEMPLATE", "")
    monkeypatch.setattr(activities, "HELM_CREATE_NAMESPACE", False)
    monkeypatch.setattr(activities, "HELM_MOCK_GPU", True)
    monkeypatch.setattr(activities, "HELM_KUBE_APISERVER", "")
    monkeypatch.setattr(activities, "HELM_KUBE_TLS_SERVER_NAME", "")
    monkeypatch.setattr(activities, "HELM_KUBE_CA_FILE", "")
    monkeypatch.setattr(activities, "HELM_KUBE_TOKEN_FILE", "")
    monkeypatch.setattr(activities, "HELM_KUBE_INSECURE_SKIP_TLS_VERIFY", False)
    monkeypatch.setattr(activities.subprocess, "run", fake_run)
    monkeypatch.setattr(activities.activity, "heartbeat", lambda _: None)

    result = asyncio.run(
        activities.run_helm_deploy(
            {"customer_id": "team-a", "tier": "standard"}
        )
    )

    assert result == (
        "Successfully rendered resources via Helm for release: tenant-team-a"
    )
    assert calls == [
        [
            "helm",
            "template",
            "tenant-team-a",
            "./helm/tenant-workload",
            "--set",
            "customerId=team-a",
            "--set",
            "tier=standard",
            "--set",
            "gpuCount=1",
            "--set",
            "mockGpu=true",
            "--namespace",
            "default",
        ]
    ]


def test_run_helm_deploy_uses_configured_namespace_and_gpu_mode(monkeypatch):
    calls = []

    def fake_run(command, capture_output, text, timeout):
        calls.append(command)
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(activities, "HELM_DRY_RUN", False)
    monkeypatch.setattr(activities, "HELM_NAMESPACE", "gpu-tenant-orchestrator")
    monkeypatch.setattr(activities, "HELM_NAMESPACE_TEMPLATE", "")
    monkeypatch.setattr(activities, "HELM_CREATE_NAMESPACE", False)
    monkeypatch.setattr(activities, "HELM_MOCK_GPU", False)
    monkeypatch.setattr(activities, "HELM_KUBE_APISERVER", "")
    monkeypatch.setattr(activities, "HELM_KUBE_TLS_SERVER_NAME", "")
    monkeypatch.setattr(activities, "HELM_KUBE_CA_FILE", "")
    monkeypatch.setattr(activities, "HELM_KUBE_TOKEN_FILE", "")
    monkeypatch.setattr(activities, "HELM_KUBE_INSECURE_SKIP_TLS_VERIFY", False)
    monkeypatch.setattr(activities.subprocess, "run", fake_run)
    monkeypatch.setattr(activities.activity, "heartbeat", lambda _: None)

    asyncio.run(
        activities.run_helm_deploy(
            {"customer_id": "team-a", "tier": "premium"}
        )
    )

    assert "--namespace" in calls[0]
    assert "gpu-tenant-orchestrator" in calls[0]
    assert "mockGpu=false" in calls[0]


def test_run_helm_deploy_can_target_tenant_namespace(monkeypatch):
    calls = []

    def fake_run(command, capture_output, text, timeout):
        calls.append(command)
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(activities, "HELM_DRY_RUN", False)
    monkeypatch.setattr(activities, "HELM_NAMESPACE", "fallback")
    monkeypatch.setattr(activities, "HELM_NAMESPACE_TEMPLATE", "tenant-{customer_id}")
    monkeypatch.setattr(activities, "HELM_CREATE_NAMESPACE", True)
    monkeypatch.setattr(activities, "HELM_MOCK_GPU", True)
    monkeypatch.setattr(activities, "HELM_KUBE_APISERVER", "")
    monkeypatch.setattr(activities, "HELM_KUBE_TLS_SERVER_NAME", "")
    monkeypatch.setattr(activities, "HELM_KUBE_CA_FILE", "")
    monkeypatch.setattr(activities, "HELM_KUBE_TOKEN_FILE", "")
    monkeypatch.setattr(activities, "HELM_KUBE_INSECURE_SKIP_TLS_VERIFY", False)
    monkeypatch.setattr(activities.subprocess, "run", fake_run)
    monkeypatch.setattr(activities.activity, "heartbeat", lambda _: None)

    asyncio.run(
        activities.run_helm_deploy(
            {"customer_id": "team-a", "tier": "premium"}
        )
    )

    assert "--namespace" in calls[0]
    assert "tenant-team-a" in calls[0]
    assert "--create-namespace" in calls[0]


def test_resolve_helm_namespace_rejects_invalid_template(monkeypatch):
    monkeypatch.setattr(activities, "HELM_NAMESPACE_TEMPLATE", "Tenant {customer_id}")

    with pytest.raises(ValueError, match="Kubernetes-safe"):
        activities.resolve_helm_namespace("team-a")


def test_build_helm_kube_flags_uses_configured_cluster_access(monkeypatch, tmp_path):
    token_file = tmp_path / "token"
    token_file.write_text("token-value\n", encoding="utf-8")
    monkeypatch.setattr(activities, "HELM_KUBE_APISERVER", "https://10.42.0.1:6443")
    monkeypatch.setattr(activities, "HELM_KUBE_TLS_SERVER_NAME", "10.43.0.1")
    monkeypatch.setattr(activities, "HELM_KUBE_CA_FILE", "/var/run/ca.crt")
    monkeypatch.setattr(activities, "HELM_KUBE_TOKEN_FILE", str(token_file))
    monkeypatch.setattr(activities, "HELM_KUBE_INSECURE_SKIP_TLS_VERIFY", False)

    assert activities.build_helm_kube_flags() == [
        "--kube-apiserver",
        "https://10.42.0.1:6443",
        "--kube-tls-server-name",
        "10.43.0.1",
        "--kube-ca-file",
        "/var/run/ca.crt",
        "--kube-token",
        "token-value",
    ]


def test_run_helm_deploy_rejects_invalid_input(monkeypatch):
    monkeypatch.setattr(activities, "HELM_DRY_RUN", False)
    monkeypatch.setattr(activities, "HELM_NAMESPACE", "default")
    monkeypatch.setattr(activities, "HELM_NAMESPACE_TEMPLATE", "")
    monkeypatch.setattr(activities, "HELM_CREATE_NAMESPACE", False)
    monkeypatch.setattr(activities, "HELM_MOCK_GPU", True)
    monkeypatch.setattr(activities, "HELM_KUBE_APISERVER", "")
    monkeypatch.setattr(activities, "HELM_KUBE_TLS_SERVER_NAME", "")
    monkeypatch.setattr(activities, "HELM_KUBE_CA_FILE", "")
    monkeypatch.setattr(activities, "HELM_KUBE_TOKEN_FILE", "")
    monkeypatch.setattr(activities, "HELM_KUBE_INSECURE_SKIP_TLS_VERIFY", False)
    monkeypatch.setattr(activities.activity, "heartbeat", lambda _: None)

    with pytest.raises(ValueError, match="customer_id and a valid tier"):
        asyncio.run(
            activities.run_helm_deploy(
                {"customer_id": "../bad", "tier": "premium"}
            )
        )


def test_run_helm_deploy_raises_when_helm_fails(monkeypatch):
    monkeypatch.setattr(activities, "HELM_DRY_RUN", False)
    monkeypatch.setattr(activities, "HELM_NAMESPACE", "default")
    monkeypatch.setattr(activities, "HELM_NAMESPACE_TEMPLATE", "")
    monkeypatch.setattr(activities, "HELM_CREATE_NAMESPACE", False)
    monkeypatch.setattr(activities, "HELM_MOCK_GPU", True)
    monkeypatch.setattr(activities, "HELM_KUBE_APISERVER", "")
    monkeypatch.setattr(activities, "HELM_KUBE_TLS_SERVER_NAME", "")
    monkeypatch.setattr(activities, "HELM_KUBE_CA_FILE", "")
    monkeypatch.setattr(activities, "HELM_KUBE_TOKEN_FILE", "")
    monkeypatch.setattr(activities, "HELM_KUBE_INSECURE_SKIP_TLS_VERIFY", False)
    monkeypatch.setattr(activities.activity, "heartbeat", lambda _: None)
    monkeypatch.setattr(
        activities.subprocess,
        "run",
        lambda *_, **__: SimpleNamespace(returncode=1, stderr="chart failed"),
    )

    with pytest.raises(RuntimeError, match="Helm execution failed: chart failed"):
        asyncio.run(
            activities.run_helm_deploy(
                {"customer_id": "team-a", "tier": "standard"}
            )
        )
