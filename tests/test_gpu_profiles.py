import pytest

from src.shared.gpu_profiles import load_gpu_profiles


def test_load_gpu_profiles_reads_profile_catalog():
    catalog = load_gpu_profiles(
        """
        {
          "profiles": {
            "standard": {
              "gpu_count": 1,
              "gpu_type": "nvidia-a10",
              "default_max_latency_ms": 100
            },
            "premium": {
              "gpu_count": 2,
              "gpu_type": "nvidia-a100",
              "default_max_latency_ms": 80
            }
          }
        }
        """
    )

    premium = catalog.get("Premium")

    assert catalog.valid_tiers() == {"standard", "premium"}
    assert catalog.gpu_counts_by_tier() == {"standard": 1, "premium": 2}
    assert premium.gpu_type == "nvidia-a100"
    assert premium.default_max_latency_ms == 80
    assert not catalog.has_mock_gpu_type()


def test_load_gpu_profiles_rejects_invalid_json():
    with pytest.raises(ValueError, match="valid JSON"):
        load_gpu_profiles("{")


def test_load_gpu_profiles_rejects_missing_required_fields():
    with pytest.raises(ValueError, match="missing required fields"):
        load_gpu_profiles('{"profiles":{"standard":{"gpu_count":1}}}')


def test_load_gpu_profiles_rejects_non_positive_gpu_count():
    with pytest.raises(ValueError, match="gpu_count must be positive"):
        load_gpu_profiles(
            """
            {
              "profiles": {
                "standard": {
                  "gpu_count": 0,
                  "gpu_type": "nvidia-a10",
                  "default_max_latency_ms": 100
                }
              }
            }
            """
        )
