# Spark Connect ML Integration Strategies on Apache Beam

This document provides a strategic architectural analysis comparing the different ways of integrating Spark Connect Machine Learning transforms (`MlRelation.Transform`) with the Calcite relational translation layer in Apache Beam.

---

## Comparison of Integration Options

```mermaid
graph TD
    subgraph Option 1: Custom RelNode
        SC1[Spark Connect ML] -->|Translate| CR[Custom RelNode: BeamMlTransformRel]
        CR -->|Planner Rules| B1[Compile to native Beam PTransform]
    end
    
    subgraph Option 2: Hybrid DAG (Escaping Calcite)
        SC2[Spark Connect ML] -->|Translate relation| R2[Calcite RelNode]
        R2 -->|Compile| PC2[Beam PCollection]
        PC2 -->|Apply native Beam transform| BT2[RunInference / ParDo]
    end
    
    subgraph Option 3: Calcite SQL UDF
        SC3[Spark Connect ML] -->|Translate| PR3[LogicalProject with ML_PREDICT UDF]
        PR3 -->|Standard Compile| B3[Beam SQL Runner]
    end
```

---

### Option 1: Custom RelNode in Calcite (The Relational Way)

Instead of escaping Calcite, we define a custom physical relational node in Calcite (e.g. `BeamMlTransformRelNode`) that represents the ML inference step.

#### What It Looks Like:
```java
public class BeamMlTransformRelNode extends AbstractRelNode implements BeamRelNode {
  private final RelNode input;
  private final SparkMLObjectRegistry.ObjectRefState modelState;
  private final MlParams params;
  
  public BeamMlTransformRelNode(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      SparkMLObjectRegistry.ObjectRefState modelState,
      MlParams params) {
    super(cluster, traits);
    this.input = input;
    this.modelState = modelState;
    this.params = params;
  }

  @Override
  public RelDataType deriveRowType() {
    // Dynamically compute schema by appending prediction / probability columns to input
    return MLSchemaUtils.deriveOutputSchema(input.getRowType(), modelState);
  }

  @Override
  public PCollection<Row> buildBeamPipeline(BeamPipelineTranslator translator) {
    PCollection<Row> upstream = translator.translate(input);
    // Apply standard Beam RunInference / ModelHandler execution
    return upstream.apply(RunInference.of(
        ModelHandlerRegistry.getHandler(modelState), 
        ModelHandlerRegistry.getSelector(params)
    ));
  }
}
```

#### Pros & Cons:
* **PRO**: **Perfect relational integration**. The entire Spark Connect plan is represented as a single Calcite `RelNode` tree, allowing global rule-based optimization (e.g. filter push-down, projecting only columns used in ML, combining projections).
* **PRO**: No surface SQL syntax is needed; the node is generated directly by the translator from the Spark Connect protobuf messages.
* **CON**: Requires subclassing Calcite `RelNode` classes and writing custom compilation rules, adding minor architectural complexity to the SQL planner.

---

### Option 2: Hybrid DAG (Escaping Calcite)

In this approach, we do not force ML operations into Calcite. The translator parses the relational sub-tree up to the ML transformation boundary, compiles that sub-tree into a native Beam `PCollection`, and then applies standard Beam `PTransform`s (like `ParDo` or `RunInference`) directly to it.

#### What It Looks Like:
Instead of returning `RelNode`, `translate` returns a hybrid wrapper `BeamPlanNode`:
```java
public interface BeamPlanNode {
  boolean isRelNode();
  RelNode getRelNode();
  
  PCollection<Row> compile(Pipeline pipeline, BeamSQLPlanner planner);
}
```
When an ML Transform is encountered:
```java
public class BeamMlTransformPlanNode implements BeamPlanNode {
  private final BeamPlanNode inputPlanNode;
  private final ObjectRefState modelState;

  public PCollection<Row> compile(Pipeline pipeline, BeamSQLPlanner planner) {
    PCollection<Row> inputPCollection = inputPlanNode.compile(pipeline, planner);
    // Run standard native Beam SDK operations
    return inputPCollection.apply(RunInference.of(modelState.getPath()));
  }
}
```

#### Pros & Cons:
* **PRO**: **Extremely simple and rapid to implement**. Leverages the complete, native Apache Beam Java SDK directly without Calcite rule boilerplate.
* **CON**: **Breaks query planning optimizations**. Once we escape Calcite, subsequent relational transformations (like SQL joins, filters, and aggregates after prediction) cannot be co-optimized by Calcite.
* **CON**: Changes the translator's primary output signature from `RelNode` to a custom wrapper structure.

---

### Option 3: LogicalProject with SQL UDF (User Defined Functions)

We represent the model prediction as a standard Calcite scalar or table-valued function (UDF) evaluated during a `LogicalProject` projection.

#### What It Looks Like:
```java
// Translator compiles the ML transform into a standard Calcite project:
// SELECT id, ML_PREDICT(features, 'model_path') as prediction FROM input
```

#### Pros & Cons:
* **PRO**: Leverages 100% vanilla Calcite logical elements out-of-the-box.
* **CON**: Multi-column handling, sparse vectors, and complex model parameter blocks (`MlParams`) are very awkward to pass inside generic SQL function argument lists.
* **CON**: Initializing heavy models (like large PyTorch/TensorFlow models) repeatedly inside volatile, lightweight SQL scalar evaluation contexts can create execution overhead.

---

## Strategic Recommendation

We recommend **Option 1 (Custom RelNode)** because:
1. It preserves **unified logical execution trees** in Calcite, keeping code optimizer-friendly.
2. As noted by the user, **no surface SQL syntax is required**. The gRPC parser translates incoming Spark ML relations directly into `BeamMlTransformRelNode` objects.
3. It allows clean integration of high-performance Beam model inference (`RunInference`) directly inside `buildBeamPipeline`.
