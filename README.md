# OK FDS

## Folder Structure


investment_funds_project/
│
├── funds/                     # Contains ONLY individual Fund implementation folders
│   ├── __init__.py            # Likely empty or minimal, facilitates fund discovery if needed
│   │
│   ├── fund_a/                # Folder for Fund A assets
│   │   ├── __init__.py
│   │   ├── fund_a.py          # Implementation for Fund A (imports from core.fund_base, core.fund_intermediate)
│   │   └── fund_a.yaml        # Configuration for Fund A
│   │
│   ├── fund_b/                # Folder for Fund B assets
│   │   ├── __init__.py
│   │   ├── fund_b.py          # Implementation for Fund B
│   │   └── fund_b.yaml        # Configuration for Fund B
│   │
│   └── ... (over 100 fund folders) # Each fund has its own folder containing .py and .yaml
│
├── core/                      # Core orchestration, logic, AND Fund base/intermediate classes
│   ├── __init__.py
│   ├── factory.py             # FundFactory class - orchestrates fund operations
│   ├── dependency_graph.py    # Logic for tracking and resolving data dependencies
│   ├── registry.py            # Implementation for the in-flight data request registry
│   │
│   ├── fund_base.py           # Defines BaseFund (ABC) - Moved from funds/
│   # Example comment in fund_base.py:
│   # """Abstract Base Class for all Fund implementations (defined in funds/ subdirectories).
│   # Ensures all concrete funds provide the necessary asynchronous methods.
│   # """
│   │
│   └── fund_intermediate.py   # Example intermediate fund classes (e.g., JapanFund) - Moved from funds/
│   # Example comment in fund_intermediate.py:
│   # """Contains intermediate fund classes like JapanFund, inheriting from BaseFund,
│   # providing shared logic for specific fund groups.
│   # """
│
├── dataprovider/              # Data Access Layer - handles all external data interactions
│   ├── __init__.py            # Exposes the UnifiedDataProvider
│   ├── interfaces.py          # Abstract Base Classes for Data Providers (IDataProvider, ILowLevelProvider)
│   ├── unified.py             # UnifiedDataProvider - central coordinator
│   │
│   ├── fund_providers/        # Category-specific providers
│   │   ├── __init__.py
│   │   └── sql_provider.py
│   │   └── api_provider.py
│   │   └── ...
│   │
│   ├── market_providers/      # Category-specific providers
│   │   ├── __init__.py
│   │   └── bloomberg_provider.py
│   │   └── external_lib_provider.py
│   │   └── ...
│   │
│   └── models.py              # Data models/schemas (e.g., Pydantic models)
│
├── caching/                   # Caching infrastructure components
│   ├── __init__.py            # Exposes CacheManager or individual cache layers
│   ├── interface.py           # Abstract Base Class for ICacheProvider
│   ├── manager.py             # Optional CacheManager
│   ├── L1_memory_cache.py     # In-process memory cache
│   ├── L2_local_cache.py      # Local persistent cache
│   └── L3_distributed_cache.py # Distributed cache client
│
├── config/                    # Global system configuration management
│   ├── __init__.py
│   ├── loader.py              # Loads global and fund-specific configurations
│   └── settings.yaml          # Global settings
│
├── utils/                     # Common utilities and helper functions
│   ├── __init__.py
│   ├── async_utils.py
│   ├── decorators.py
│   └── monitoring.py
│
├── tests/                     # Unit and Integration Tests (structure mirrors main code)
│   ├── __init__.py
│   ├── test_core/             # Tests for core components including base/intermediate funds
│   │   ├── test_factory.py
│   │   ├── test_fund_base.py
│   │   └── test_fund_intermediate.py # Test intermediate classes
│   ├── test_funds/            # Tests for specific fund implementations
│   │   └── test_fund_a/       # Tests specific to Fund A
│   │       └── test_fund_a.py
│   ├── test_dataprovider/
│   ├── test_caching/
│   └── ...
│
├── scripts/                   # Entry points for batch jobs, ad-hoc analyses, etc.
│   └── run_daily_nav.py
│   └── update_guidelines.py
│
└── main.py                    # Main application entry point (if applicable)