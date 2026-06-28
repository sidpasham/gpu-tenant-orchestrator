import json
import re
from dataclasses import dataclass
from typing import Any


PROFILE_NAME_PATTERN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")


@dataclass(frozen=True)
class GpuProfile:
    name: str
    gpu_count: int
    gpu_type: str
    default_max_latency_ms: int

    @classmethod
    def from_mapping(cls, name: str, raw_profile: dict[str, Any]) -> "GpuProfile":
        normalized_name = name.lower()
        if not PROFILE_NAME_PATTERN.fullmatch(normalized_name):
            raise ValueError(f"Invalid GPU profile name: {name}")

        required_fields = [
            "gpu_count",
            "gpu_type",
            "default_max_latency_ms",
        ]
        missing_fields = [
            field_name
            for field_name in required_fields
            if field_name not in raw_profile
        ]
        if missing_fields:
            raise ValueError(
                f"GPU profile {name} is missing required fields: "
                + ", ".join(sorted(missing_fields))
            )

        gpu_count = int(raw_profile["gpu_count"])
        default_max_latency_ms = int(raw_profile["default_max_latency_ms"])
        if gpu_count <= 0:
            raise ValueError(f"GPU profile {name} gpu_count must be positive")
        if default_max_latency_ms <= 0:
            raise ValueError(
                f"GPU profile {name} default_max_latency_ms must be positive"
            )

        gpu_type = str(raw_profile["gpu_type"]).lower()
        if not PROFILE_NAME_PATTERN.fullmatch(gpu_type):
            raise ValueError(
                f"GPU profile {name} gpu_type must contain lowercase letters, "
                "numbers, and hyphens"
            )

        return cls(
            name=normalized_name,
            gpu_count=gpu_count,
            gpu_type=gpu_type,
            default_max_latency_ms=default_max_latency_ms,
        )


class GpuProfileCatalog:
    def __init__(self, profiles: dict[str, GpuProfile]) -> None:
        if not profiles:
            raise ValueError("At least one GPU profile must be configured")
        self.profiles = profiles

    def get(self, tier: str) -> GpuProfile | None:
        if not isinstance(tier, str):
            return None
        return self.profiles.get(tier.lower())

    def valid_tiers(self) -> set[str]:
        return set(self.profiles)

    def gpu_counts_by_tier(self) -> dict[str, int]:
        return {
            tier: profile.gpu_count
            for tier, profile in self.profiles.items()
        }

    def has_mock_gpu_type(self) -> bool:
        return any(profile.gpu_type == "mock" for profile in self.profiles.values())


def load_gpu_profiles(config_text: str) -> GpuProfileCatalog:
    try:
        raw_config = json.loads(config_text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"GPU profile config must be valid JSON: {exc}") from exc

    raw_profiles = raw_config.get("profiles") if isinstance(raw_config, dict) else None
    if not isinstance(raw_profiles, dict):
        raise ValueError("GPU profile config must contain a profiles object")

    profiles = {
        profile_name.lower(): GpuProfile.from_mapping(profile_name, raw_profile)
        for profile_name, raw_profile in raw_profiles.items()
        if isinstance(raw_profile, dict)
    }
    if len(profiles) != len(raw_profiles):
        raise ValueError("Each GPU profile entry must be an object")
    return GpuProfileCatalog(profiles)
