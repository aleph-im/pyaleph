KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024

MINUTE = 60
HOUR = 60 * MINUTE

PRICE_AGGREGATE_OWNER = "0xFba561a84A537fCaa567bb7A2257e7142701ae2A"
PRICE_AGGREGATE_KEY = "pricing"
PRICE_PRECISION = 18
DEFAULT_PRICE_AGGREGATE = {
    "program": {
        "price": {
            "storage": {"payg": "0.000000977", "holding": "0.05"},
            "compute_unit": {"payg": "0.011", "holding": "200"},
        },
        "tiers": [
            {"id": "tier-1", "compute_units": 1},
            {"id": "tier-2", "compute_units": 2},
            {"id": "tier-3", "compute_units": 4},
            {"id": "tier-4", "compute_units": 6},
            {"id": "tier-5", "compute_units": 8},
            {"id": "tier-6", "compute_units": 12},
        ],
        "compute_unit": {
            "vcpus": 1,
            "disk_mib": 2048,
            "memory_mib": 2048,
        },
    },
    "storage": {"price": {"storage": {"holding": "0.333333333"}}},
    "instance": {
        "price": {
            "storage": {"payg": "0.000000977", "holding": "0.05"},
            "compute_unit": {"payg": "0.055", "holding": "1000"},
        },
        "tiers": [
            {"id": "tier-1", "compute_units": 1},
            {"id": "tier-2", "compute_units": 2},
            {"id": "tier-3", "compute_units": 4},
            {"id": "tier-4", "compute_units": 6},
            {"id": "tier-5", "compute_units": 8},
            {"id": "tier-6", "compute_units": 12},
        ],
        "compute_unit": {
            "vcpus": 1,
            "disk_mib": 20480,
            "memory_mib": 2048,
        },
    },
    "web3_hosting": {"price": {"fixed": 50, "storage": {"holding": "0.333333333"}}},
    "program_persistent": {
        "price": {
            "storage": {"payg": "0.000000977", "holding": "0.05"},
            "compute_unit": {"payg": "0.055", "holding": "1000"},
        },
        "tiers": [
            {"id": "tier-1", "compute_units": 1},
            {"id": "tier-2", "compute_units": 2},
            {"id": "tier-3", "compute_units": 4},
            {"id": "tier-4", "compute_units": 6},
            {"id": "tier-5", "compute_units": 8},
            {"id": "tier-6", "compute_units": 12},
        ],
        "compute_unit": {
            "vcpus": 1,
            "disk_mib": 20480,
            "memory_mib": 2048,
        },
    },
    "instance_gpu_premium": {
        "price": {
            "storage": {"payg": "0.000000977"},
            "compute_unit": {"payg": "0.56"},
        },
        "tiers": [
            {
                "id": "tier-1",
                "vram": 81920,
                "model": "A100",
                "compute_units": 16,
            },
            {
                "id": "tier-2",
                "vram": 81920,
                "model": "H100",
                "compute_units": 24,
            },
        ],
        "compute_unit": {
            "vcpus": 1,
            "disk_mib": 61440,
            "memory_mib": 6144,
        },
    },
    "instance_confidential": {
        "price": {
            "storage": {"payg": "0.000000977", "holding": "0.05"},
            "compute_unit": {"payg": "0.11", "holding": "2000"},
        },
        "tiers": [
            {"id": "tier-1", "compute_units": 1},
            {"id": "tier-2", "compute_units": 2},
            {"id": "tier-3", "compute_units": 4},
            {"id": "tier-4", "compute_units": 6},
            {"id": "tier-5", "compute_units": 8},
            {"id": "tier-6", "compute_units": 12},
        ],
        "compute_unit": {
            "vcpus": 1,
            "disk_mib": 20480,
            "memory_mib": 2048,
        },
    },
    "instance_gpu_standard": {
        "price": {
            "storage": {"payg": "0.000000977"},
            "compute_unit": {"payg": "0.28"},
        },
        "tiers": [
            {
                "id": "tier-1",
                "vram": 20480,
                "model": "RTX 4000 ADA",
                "compute_units": 3,
            },
            {
                "id": "tier-2",
                "vram": 24576,
                "model": "RTX 3090",
                "compute_units": 4,
            },
            {
                "id": "tier-3",
                "vram": 24576,
                "model": "RTX 4090",
                "compute_units": 6,
            },
            {
                "id": "tier-4",
                "vram": 49152,
                "model": "L40S",
                "compute_units": 12,
            },
        ],
        "compute_unit": {
            "vcpus": 1,
            "disk_mib": 61440,
            "memory_mib": 6144,
        },
    },
}

SETTINGS_AGGREGATE_OWNER = "0xFba561a84A537fCaa567bb7A2257e7142701ae2A"
SETTINGS_AGGREGATE_KEY = "settings"
DEFAULT_SETTINGS_AGGREGATE = {
    "compatible_gpus": [
        {
            "name": "AD102GL [L40S]",
            "model": "L40S",
            "vendor": "NVIDIA",
            "device_id": "10de:26b9",
        },
        {
            "name": "GB202 [GeForce RTX 5090]",
            "model": "RTX 5090",
            "vendor": "NVIDIA",
            "device_id": "10de:2685",
        },
        {
            "name": "GB202 [GeForce RTX 5090 D]",
            "model": "RTX 5090",
            "vendor": "NVIDIA",
            "device_id": "10de:2687",
        },
        {
            "name": "AD102 [GeForce RTX 4090]",
            "model": "RTX 4090",
            "vendor": "NVIDIA",
            "device_id": "10de:2684",
        },
        {
            "name": "AD102 [GeForce RTX 4090 D]",
            "model": "RTX 4090",
            "vendor": "NVIDIA",
            "device_id": "10de:2685",
        },
        {
            "name": "GA102 [GeForce RTX 3090]",
            "model": "RTX 3090",
            "vendor": "NVIDIA",
            "device_id": "10de:2204",
        },
        {
            "name": "GA102 [GeForce RTX 3090 Ti]",
            "model": "RTX 3090",
            "vendor": "NVIDIA",
            "device_id": "10de:2203",
        },
        {
            "name": "AD104GL [RTX 4000 SFF Ada Generation]",
            "model": "RTX 4000 ADA",
            "vendor": "NVIDIA",
            "device_id": "10de:27b0",
        },
        {
            "name": "AD104GL [RTX 4000 Ada Generation]",
            "model": "RTX 4000 ADA",
            "vendor": "NVIDIA",
            "device_id": "10de:27b2",
        },
        {
            "name": "GH100 [H100]",
            "model": "H100",
            "vendor": "NVIDIA",
            "device_id": "10de:2336",
        },
        {
            "name": "GH100 [H100 NVSwitch]",
            "model": "H100",
            "vendor": "NVIDIA",
            "device_id": "10de:22a3",
        },
        {
            "name": "GH100 [H100 CNX]",
            "model": "H100",
            "vendor": "NVIDIA",
            "device_id": "10de:2313",
        },
        {
            "name": "GH100 [H100 SXM5 80GB]",
            "model": "H100",
            "vendor": "NVIDIA",
            "device_id": "10de:2330",
        },
        {
            "name": "GH100 [H100 PCIe]",
            "model": "H100",
            "vendor": "NVIDIA",
            "device_id": "10de:2331",
        },
        {
            "name": "GA100",
            "model": "A100",
            "vendor": "NVIDIA",
            "device_id": "10de:2080",
        },
        {
            "name": "GA100",
            "model": "A100",
            "vendor": "NVIDIA",
            "device_id": "10de:2081",
        },
        {
            "name": "GA100 [A100 SXM4 80GB]",
            "model": "A100",
            "vendor": "NVIDIA",
            "device_id": "10de:20b2",
        },
        {
            "name": "GA100 [A100 PCIe 80GB]",
            "model": "A100",
            "vendor": "NVIDIA",
            "device_id": "10de:20b5",
        },
        {
            "name": "GA100 [A100X]",
            "model": "A100",
            "vendor": "NVIDIA",
            "device_id": "10de:20b8",
        },
    ],
    "community_wallet_address": "0x5aBd3258C5492fD378EBC2e0017416E199e5Da56",
    "community_wallet_timestamp": 1739301770,
}

STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT = 22196000  # 22388870
STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP = 1743731879  # 1746101025

MAX_FILE_SIZE = 100 * MiB
MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE = 25 * MiB
# MAX_UPLOAD_FILE_SIZE = 1000 * MiB (not used?)
