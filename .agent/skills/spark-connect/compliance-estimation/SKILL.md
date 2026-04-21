# Spark Connect Compliance Estimation and Analysis

**Purpose**: Guidance on how to produce a compliance report, perform light design work, and estimate effort for Spark Connect compliance categories, including assessment of AI-assisted development ease.

**How to Use**:
1. **Produce Compliance Report**:
   Run the predefined Gradle task to get the current compliance statistics:
   ```bash
   ./gradlew :sparkconnect:computeComplianceStats
   ```
   This will output a summary of compliance by category and subcategory.

2. **Design Work for Categories**:
   For each category (or at least the non-compliant ones), document the following:
   - **Scope**: What functionality is tested by the category/subcategory.
   - **Workload Criticality**: Which workloads that functionality is critical for, and which it is not important for.
   - **Implementation Method**: A proposed implementation method for achieving compliance.
   - **Effort Estimate**: Relative effort of implementation using T-shirt sizes (S, M, L, XL).
   - **AI Ease Estimate**: How easy it might be for AI-assisted development to quickly achieve compliance (High, Medium, Low).

3. **Evaluate Workload Styles**:
   Group categories into collections to enable specific styles of workloads (e.g., Core Batch Analytics & BI, Streaming Data Pipelines, Data Science & ML Prep) and estimate the combined effort and AI feasibility.

**Important Rule for Reporting**:
- Always include all categories in the analysis if requested by the user, or focus on non-compliant ones to identify the biggest gaps.
- Use T-shirt sizes (S, M, L, XL) for effort estimation to keep it high-level unless a more granular scale is requested.
- Use the **AI Ease Scale** (High, Medium, Low) to help prioritize tasks that can be accelerated by AI.
- Highlight core SQL compliance as the major bottleneck if it is still at low compliance, as it usually underpins all other functionalities.
- **Reference Test Descriptions**: Always reference the descriptions of tests in `sparkconnect/compliance_tests_description.md` when producing or refining estimates to understand the specific functionality and gaps.
