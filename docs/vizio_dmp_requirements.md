# Vizio DMP / Audience 1PD Use Case

## Project Context

You are assisting with the design and architecture of a **pseudo-DMP (Data Management Platform)** solution for Vizio/Inscape using the Akkio platform. The goal is to enable customers to self-serve audience creation and activation without requiring extensive managed services.

---

## Use Case Overview

### Business Objective
Transform Akkio UI into a self-serve DMP platform where customers can:
- Query and segment Inscape (Vizio) data within their own tenancy
- Import their own first-party data (1PD)
- Optionally run Look-Alike (LAL) models for audience scaling
- Push segments to activation platforms with automated refresh
- Enable Inscape to monitor usage and collect reporting from activation platforms

### Current State (Problem)
- Custom audiences are currently created via **managed service only**
- Self-serve requires: (A) licensing data AND (B) targeting use case grant (rarely approved)
- Goal: Make custom audiences accessible to broader customer base without manual overhead

---

## Platform Modes

### 1. Internal Full-Serve Mode
- Full audience activation capabilities
- Activation via LiveRamp or direct to Vizio Ads
- Complete access to Inscape data and tools

### 2. External Self-Serve Mode
- Platform pre-loaded with Inscape data
- Clients bring their own 1PD into the platform
- 1PD is married/matched to Inscape data
- Clients self-activate to their preferred platforms

---

## Activation Partners (Distribution Endpoints)

The following are Vizio's top 10 activation partners that segments need to be distributed to:

| Partner | Notes |
|---------|-------|
| LiveRamp (Connect) | Primary distribution rail |
| Meta | Hashed PII distribution |
| The Trade Desk | Includes Walmart Connect; Hashed PII |
| TikTok | |
| Google | DV360 and GAM |
| Madhive | |
| Cadent | |
| Freewheel | |
| Spring Serve | |
| Beeswax | |
| MiQ | |
| Magnite | |
| VIZIO Ads | Direct activation |

---

## Identity & Distribution Architecture

### ID Types & Resolution

| ID Type | Description | Usage |
|---------|-------------|-------|
| **TVID** | Vizio TV identifier | Core Inscape identifier |
| **Hashed PII** | SHA256 of email, phone, zip | Distribution to Meta, TTD, etc. |
| **IP Address** | From TV connection | Linkage to household |
| **Email (hashed)** | From TV registration | Identity matching |
| **Encoded Person ID** | Tuple of PII information | Required on top of TVID for person-level targeting |

### ID Resolution Challenge
- TV data and first-party data may be **disjoint** (no direct connection)
- Solution: Link IP/email to TVID, then layer encoded person ID on top

### Distribution Options

**Without ID Spine:**
- Use encoded ID (tuple of PII information)
- Link IP/email IDs to TVID
- Requires encoded person ID layer

**With ID Spine (e.g., TransUnion):**
- Horizon example: TU spine, self-send
- PlatformOne: Syndication ID/group, data share via Snowflake

### LiveRamp Integration
- Use LUID directly to LiveRamp (potentially with Experian)
- LiveRamp API accepts hashed PII
- Delivery methods:
  - Package and drop to S3 (pull model)
  - Direct API integration
- Base module for distribution with client APIs

---

## 1PD Integration Architecture

### Ingestion Methods
1. **Clean Room Connectors** (e.g., Snowflake, AWS Clean Rooms)
2. **Delta Share** (Databricks native)
3. **Flat File Drops** (S3, SFTP)
4. **Cloud Infrastructure Connectors** (direct warehouse connections)

### Integration Flow
```
Client 1PD → Databricks (ingestion layer) → ID Matching (to TV_ID) → Audience Build → Distribution
```

### Key Requirement
- Must know the **mapping to TV_ID** on Vizio side
- Once mapped, audience build workflow is identical to existing internal workflows

---

## Outstanding Questions to Resolve

1. **Data Residency**: Confirm Vizio will house all data in their warehouse (compute ownership)
2. **ID Spine Requirement**: Determine if clients need to purchase their own ID spine or use Vizio's
3. **User Flow Mock-up**: Define how 1PD gets into the platform (the user journey)

---

## Architecture Diagram Requirements

When building architecture or user flow diagrams, include:

### Components
- Client tenancy/workspace in Akkio
- Inscape data layer (pre-loaded)
- 1PD ingestion endpoints (clean room, delta share, flat file)
- ID resolution/matching service (TVID mapping)
- Audience builder (query/segmentation)
- LAL model service (optional scaling)
- Distribution orchestration layer
- Activation platform connectors (LiveRamp, direct APIs)
- Reporting/monitoring dashboard

### Data Flows
- 1PD ingestion path
- ID matching/resolution flow
- Audience segment creation
- Distribution to activation platforms
- Reporting/usage data return path

### User Roles
- Internal Vizio users (full-serve)
- External client users (self-serve)
- Inscape admin (monitoring/reporting)

---

## Technical Assumptions

- Databricks is the underlying data platform
- Akkio provides the UI/workflow layer
- LiveRamp is the primary distribution rail for multi-platform reach
- Hashed PII (SHA256) is the standard for platform distribution
- Automated refresh requires scheduling/orchestration capability