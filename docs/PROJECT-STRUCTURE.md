# Project Structure Index

This document provides a complete index of the project structure.

## Directory Overview

### 📚 Documentation (`/docs`)
- **COMPREHENSIVE-GUIDE.md** - Complete 100-step learning path
- **getting-started/** - Steps 1-10: Fundamentals and setup
- **intermediate/** - Steps 11-30: Configuration and connections
- **advanced/** - Steps 31-70: Advanced patterns and optimization
- **expert/** - Steps 71-90: Performance tuning and best practices
- **production/** - Steps 91-100: Production deployment

### 💻 Source Code (`/src`)
- **connectors/** - Data source connector implementations
  - `base_connector.py` - Abstract base class
  - `redshift_connector.py` - Amazon Redshift
  - `teradata_connector.py` - Teradata Vantage  
  - `bigquery_connector.py` - Google BigQuery
  - `connection_factory.py` - Factory pattern implementation
- **transformations/** - Data transformation utilities
  - `data_transformer.py` - Reusable transformation functions
- **iceberg/** - Apache Iceberg operations
  - `table_manager.py` - Table lifecycle management
- **utils/** - Utility modules (extensible)

### ⚙️ Configuration (`/config`)
- **redshift/** - Redshift connection configurations
- **teradata/** - Teradata connection configurations
- **bigquery/** - BigQuery connection configurations
- **iceberg/** - Iceberg table properties
- **glue/** - AWS Glue job settings

### 📦 Libraries (`/lib`)
- **validation/** - Data quality validators
  - `data_quality.py` - Comprehensive validation framework
- **logging/** - Enhanced logging
  - `glue_logger.py` - CloudWatch-integrated logger
- **monitoring/** - Metrics and monitoring (extensible)

### 🔧 Scripts (`/scripts`)
- **setup/** - Environment setup and validation
  - `validate-environment.py` - Environment checker
- **deployment/** - Deployment automation
  - `deploy-libraries.sh` - Library deployment script
- **maintenance/** - Maintenance tasks (extensible)

### 📝 Templates (`/templates`)
- **glue-jobs/** - Reusable job templates
  - `base-etl-job.py` - Abstract ETL job class
- **etl-pipelines/** - Pipeline templates (extensible)
- **iam-policies/** - IAM policy templates (extensible)

### 💡 Examples (`/examples`)
- **basic/** - Getting started examples
  - `cross-source-join.py` - Multi-source join example
- **intermediate/** - Intermediate patterns
  - `iceberg-merge.py` - MERGE operation example
- **advanced/** - Production patterns (extensible)

### 🧪 Tests (`/tests`)
- **unit/** - Unit tests (to be implemented)
- **integration/** - Integration tests (to be implemented)
- **e2e/** - End-to-end tests (to be implemented)

## File Count Summary

- **Python Source Files**: 15+
- **Configuration Files**: 5+
- **Documentation Files**: 100+ pages
- **Scripts**: 3+
- **Examples**: 2+
- **README Files**: 6+

## Organization Principles

1. **Separation of Concerns** - Each directory has a single, well-defined purpose
2. **Reusability** - Code is modular and designed for reuse
3. **Documentation-First** - Comprehensive docs alongside code
4. **Configuration Management** - Centralized, versioned configs
5. **Minimal Root Files** - Only essential files in root directory

## Navigation Guide

**New Users**: Start with `docs/COMPREHENSIVE-GUIDE.md`

**Developers**: 
1. Review `src/` for code modules
2. Check `examples/` for usage patterns
3. Consult `templates/` for job structures

**DevOps/SRE**:
1. Review `config/` for environment settings
2. Check `scripts/deployment/` for automation
3. Consult `docs/production/` for deployment guide

**Data Engineers**:
1. Start with `examples/`
2. Customize using `src/` modules
3. Configure via `config/` files
4. Deploy using `scripts/`

## Extension Points

Add new functionality in these areas:
- `src/utils/` - New utility modules
- `lib/monitoring/` - Monitoring integrations
- `templates/iam-policies/` - Security policies
- `tests/` - Test coverage
- `examples/advanced/` - Complex patterns
