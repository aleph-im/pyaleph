from enum import Enum
from typing import Dict, Union

KiB = 1024
MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024

MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR


class ProductPriceType(str, Enum):
    STORAGE = "storage"
    WEB3_HOSTING = "web3_hosting"
    PROGRAM = "program"
    PROGRAM_PERSISTENT = "program_persistent"
    INSTANCE = "instance"
    INSTANCE_GPU_PREMIUM = "instance_gpu_premium"
    INSTANCE_CONFIDENTIAL = "instance_confidential"
    INSTANCE_GPU_STANDARD = "instance_gpu_standard"


PRICE_AGGREGATE_OWNER = "0xFba561a84A537fCaa567bb7A2257e7142701ae2A"
PRICE_AGGREGATE_KEY = "pricing"
PRICE_PRECISION = 18
DEFAULT_PRICE_AGGREGATE: Dict[Union[ProductPriceType, str], dict] = {
    ProductPriceType.PROGRAM: {
        "price": {
            "storage": {
                "payg": "0.000000977",
                "holding": "0.05",
                "credit": "0.977",
            },
            "compute_unit": {"payg": "0.011", "holding": "200", "credit": "11000"},
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
    ProductPriceType.STORAGE: {
        "price": {
            "storage": {"holding": "0.333333333", "credit": "0.17967489030626108"}
        }
    },
    ProductPriceType.INSTANCE: {
        "price": {
            "storage": {
                "payg": "0.000000977",
                "holding": "0.05",
                "credit": "0.17967489030626108",
            },
            "compute_unit": {"payg": "0.055", "holding": "1000", "credit": "14250"},
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
    ProductPriceType.WEB3_HOSTING: {
        "price": {
            "fixed": 50,
            "storage": {"holding": "0.333333333", "credit": "0.17967489030626108"},
        }
    },
    ProductPriceType.PROGRAM_PERSISTENT: {
        "price": {
            "storage": {
                "payg": "0.000000977",
                "holding": "0.05",
                "credit": "0.977",
            },
            "compute_unit": {"payg": "0.055", "holding": "1000", "credit": "55000"},
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
    ProductPriceType.INSTANCE_GPU_PREMIUM: {
        "price": {
            "storage": {"payg": "0.000000977", "credit": "0.17967489030626108"},
            "compute_unit": {"payg": "0.56", "holding": "560", "credit": "86250"},
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
    ProductPriceType.INSTANCE_CONFIDENTIAL: {
        "price": {
            "storage": {
                "payg": "0.000000977",
                "holding": "0.05",
                "credit": "0.17967489030626108",
            },
            "compute_unit": {"payg": "0.11", "holding": "2000", "credit": "28500"},
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
    ProductPriceType.INSTANCE_GPU_STANDARD: {
        "price": {
            "storage": {"payg": "0.000000977", "credit": "0.17967489030626108"},
            "compute_unit": {"payg": "0.28", "holding": "280", "credit": "43125"},
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

STORE_AND_PROGRAM_COST_CUTOFF_HEIGHT = 22196000
STORE_AND_PROGRAM_COST_CUTOFF_TIMESTAMP = 1743775079

# Cutoff for STORE messages requiring credit-only payment
# After this timestamp, STORE messages must use credit payment (no holding tier)
# and the 25MB free file exception no longer applies
CREDIT_ONLY_CUTOFF_TIMESTAMP = 1798761600  # 2027-01-01 00:00:00 UTC

# Credit precision change: 1 USD = 1,000,000 credits (previously 100 credits)
# Messages before this timestamp have amounts in old format (need 10,000x multiplier)
CREDIT_PRECISION_CUTOFF_TIMESTAMP = 1769990400  # 2026-02-02 00:00:00 UTC
CREDIT_PRECISION_MULTIPLIER = 10000

MAX_FILE_SIZE = 100 * MiB
MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE = 25 * MiB
# MAX_UPLOAD_FILE_SIZE = 1000 * MiB (not used?)
MIN_STORE_COST_MIB = 25  # Minimum MiB cost for pure STORE messages
MIN_CREDIT_COST_PER_HOUR = (
    1  # Minimum cost per hour in credits for instances and volumes
)
