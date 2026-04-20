# Spark Compliance Test Suite Descriptions

This report describes every test in the Spark compliance suite, organized by test file and class.

## File: `python/pyspark/errors/tests/connect/test_parity_traceback.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `TracebackSqlConnectTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_datasource_analysis`
Verifies that an `AnalysisException` raised by a custom data source during analysis includes the expected user code in the traceback.

#### `test_datasource_execution`
Verifies that a `PythonException` raised by a custom data source during execution includes the expected user code in the traceback.

#### `test_udf`
Tests that tracebacks for errors in UDFs correctly identify the line where the UDF was defined or called.

#### `test_udtf_analysis`
Verifies that an `AnalysisException` raised by a custom UDTF during analysis includes the expected user code in the traceback.

#### `test_udtf_execution`
Verifies that a `PythonException` raised by a custom UDTF during execution includes the expected user code in the traceback.

## File: `python/pyspark/logger/tests/connect/test_parity_logger.py`

### Class: `LoggerParityTests`

#### `test_apply_schema`
Calls `super().test_apply_schema()`, presumably testing logger schema application.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_dataframe_query_context_logger`
Verifies that errors in DataFrames (like division by zero) are correctly logged with query context (fragment, error class, stacktrace).

#### `test_log_error`
Verifies that the logger correctly logs error messages with context and without exceptions.

#### `test_log_exception`
Verifies that the logger correctly logs exception messages with context and exception details.

#### `test_log_exception_with_stacktrace`
Verifies that the logger correctly logs exception messages with stacktraces, checking the structure of the stacktrace frames.

#### `test_log_info`
Verifies that the logger correctly logs info messages with context and without exceptions.

#### `test_log_info_with_exception`
Verifies that the logger correctly logs info messages with exception details when `exc_info=True` is passed.

#### `test_log_structure`
Verifies that log messages contain required keys: "ts", "level", "logger", "msg", "context".

#### `test_log_warning`
Verifies that the logger correctly logs warning messages with context and without exceptions.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/ml/tests/connect/test_connect_cache.py`

### Class: `MLConnectCacheTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_cleanup_ml_cache`
This test verifies the functionality of the machine learning model cache on the Spark client. It trains multiple `LinearSVC` models to populate the cache, verifies the cache contents, checks that deleting a Python reference reduces the cache size, and finally confirms that calling `_cleanup_ml_cache` clears all models from the remote cache.

#### `test_delete_model`
This test performs a detailed check on how reference counting and explicit deletion affect the remote ML model cache. It tracks the reference count of a trained model and its summary object, demonstrates that copying a model increments the remote reference count without duplicating the cached model, and verifies that the model is only removed from the remote cache when all local Python references to it are deleted.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/ml/tests/connect/test_connect_classification.py`

### Class: `ClassificationTestsOnConnect`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_binary_classes_logistic_regression`
This test trains a logistic regression model (LORV2) on binary classification data and validates its predictions and probability outputs against expected values. It tests the transformation on both Spark DataFrames and local Pandas DataFrames, ensuring that applying the transformation does not mutate the input Pandas DataFrame. It also verifies that disabling the probability column prevents it from appearing in the output.

#### `test_multi_classes_logistic_regression`
This test trains a logistic regression model (LORV2) on multi-class classification data (labels 0, 1, 2) and validates its predictions and probability outputs against expected values. It verifies that the transformation produces the correct results when applied to both Spark DataFrames and local Pandas DataFrames.

#### `test_save_load`
This test comprehensively verifies the save and load functionality for both the logistic regression estimator and its trained model, using both local paths and file system paths. It checks that parameters are preserved across save/load cycles and that the saved PyTorch model can be loaded directly by PyTorch and produces consistent probability predictions compared to the Spark model.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/ml/tests/connect/test_connect_evaluation.py`

### Class: `EvaluationTestsOnConnect`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_binary_classifier_evaluator`
This test verifies the `BinaryClassificationEvaluator` by calculating area under ROC and PR curves for a small test dataset and comparing the results to expected values, both for Spark DataFrames and local Pandas DataFrames. It also verifies that the evaluator can be saved to a local path and successfully reloaded, maintaining its configured metric name.

#### `test_multiclass_classifier_evaluator`
This test verifies the `MulticlassClassificationEvaluator` by computing the accuracy of a set of predictions against labels and comparing it to an expected value, testing with both Spark DataFrames and local Pandas DataFrames. It also ensures that the evaluator can be saved locally and reloaded as a `RegressionEvaluator`.

#### `test_regressor_evaluator`
This test verifies the `RegressionEvaluator` by calculating MSE, RMSE, and R2 metrics for a specific dataset and comparing them to expected hardcoded values, supporting both Spark DataFrames and local Pandas DataFrames. It also checks that the evaluator can be saved and reloaded locally.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/ml/tests/connect/test_connect_feature.py`

### Class: `FeatureTestsOnConnect`

#### `test_array_assembler`
This test evaluates the `ArrayAssembler` feature transformer. It verifies that it correctly combines multiple input columns into a single array column for both Spark and Pandas DataFrames, handles invalid values by 'keeping' them (converting NaN to None in Spark UDF output), and raises an exception when instructed to fail on invalid input. It also verifies local save and reload functionality.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_max_abs_scaler`
This test verifies the `MaxAbsScaler` by training it on a small dataset and asserting that the scaled features match expected values, ensuring the input DataFrame is not mutated during the process. It tests operations on both Spark and local Pandas DataFrames and confirms that both the estimator and the fitted model can be saved and reloaded locally while preserving state like scale values and sample counts.

#### `test_standard_scaler`
This test verifies the `StandardScaler` by training it on a dataset and asserting that the scaled output matches expected values computed with standard deviation and mean. It validates this on both Spark DataFrames and local Pandas DataFrames, checks that the transformation doesn't mutate the input, and verifies that the estimator and model state are preserved when saved to and loaded from a local path.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/ml/tests/connect/test_connect_function.py`

### Class: `ReusedMixedTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SparkConnectMLFunctionTests`

#### `test_array_vector_conversion`
This test verifies the conversion functions between arrays and vectors (`array_to_vector` and `vector_to_array`) in Spark Connect ML functions by comparing the results against traditional Spark operations. It checks that arrays are correctly converted to vectors and that vectors are converted back to arrays with specified element types like 'float32' and 'float64'.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/ml/tests/connect/test_connect_model_offloading.py`

### Class: `ModelOffloadingTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_fp_growth_offloading`
This test verifies that an `FPGrowth` model can be offloaded from the remote cache and still be usable. It trains the model, checks that it is cached, explicitly deletes it from the cache with `evict_only=True`, and then verifies that calling methods on the model still works by automatically reloading it.

#### `test_lda_offloading`
This test verifies offloading for `LDAModel` and its local variant `LocalLDAModel`. It trains a distributed LDA model, verifies it is in the cache, converts it to a local model, verifies both are cached, evicts both from the cache, and finally checks that both can still execute their transformation logic, demonstrating automatic reloading.

#### `test_linear_regression_offloading`
This test verifies model offloading for `LinearRegressionModel`. It fits a linear regression model, checks that it appears in the client's ML cache, evicts the model from the cache, and verifies that accessing the model's summary and making predictions still works as expected.

#### `test_linear_svc_offloading`
This test verifies model offloading for `LinearSVCModel`. It fits a Linear SVC model, verifies that the model is stored in the remote cache, evicts the model, and then confirms that accessing the model's summary and calling `predict` still functions correctly by reloading the model when needed.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/ml/tests/connect/test_connect_pipeline.py`

### Class: `PipelineTestsOnConnect`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_pipeline`
This test verifies the end-to-end functionality of an ML pipeline containing a `StandardScaler` and a Logistic Regression model (`LORV2`). It trains the pipeline, validates predictions on both Spark and Pandas DataFrames, demonstrates that a pipeline can be nested within another pipeline, and tests extensive local save and reload functionality for both the pipeline estimator and the fitted pipeline model, checking that stage parameters are preserved.

#### `test_pipeline_copy`
Validates that copying a pipeline with parameters works and doesn't affect the original.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/ml/tests/connect/test_connect_summarizer.py`

### Class: `SummarizerTestsOnConnect`

#### `test_summarize_dataframe`
Tests that summarize_dataframe produces correct results for both Spark DataFrame and local Pandas DataFrame.

## File: `python/pyspark/ml/tests/connect/test_connect_tuning.py`

### Class: `CrossValidatorTestsOnConnect`

#### `test_copy`
Tests that CrossValidator and CrossValidatorModel copy methods work and copy all parameters.

#### `test_crossvalidator_on_pipeline`
Tests CrossValidator with a pipeline, including fitting, transformation accuracy, and save/load (local and torch).

#### `test_crossvalidator_with_fold_col`
Tests CrossValidator with a specific fold column.

#### `test_fit_maximize_metric`
Validates that CrossValidator correctly identifies the model with zero induced error by maximizing the R-squared evaluation metric.

#### `test_fit_minimize_metric`
Validates that CrossValidator correctly identifies the model with zero induced error by minimizing the RMSE evaluation metric.

#### `test_gen_avg_and_std_metrics`
Tests internal helper method _gen_avg_and_std_metrics.

## File: `python/pyspark/ml/tests/connect/test_legacy_mode_classification.py`

### Class: `ClassificationTests`

#### `test_binary_classes_logistic_regression`
Confirms binary logistic regression fitting and transformation accuracy, ensuring model stability and that input data frames remain unmutated.

#### `test_multi_classes_logistic_regression`
Validates multi-class logistic regression fitting and transformation.

#### `test_save_load`
Tests save and load for LORV2 and LORV2Model (local, FS, and raw torch model).

#### `test_assert_classic_mode`
*Note: Could not find definition in parity or base files.*

### Class: `ReusedSQLTestCase`

#### `test_assert_classic_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/ml/tests/connect/test_legacy_mode_evaluation.py`

### Class: `EvaluationTests`

#### `test_binary_classifier_evaluator`
Verifies metric consistency for binary classification (AUROC, AUPRC) across local Pandas-based computation and Spark Connect.

#### `test_multiclass_classifier_evaluator`
Verifies metric consistency for multiclass classification (accuracy).

#### `test_regressor_evaluator`
Verifies metric consistency for regression (MSE, RMSE, R2).

#### `test_assert_classic_mode`
*Note: Could not find definition in parity or base files.*

### Class: `ReusedSQLTestCase`

#### `test_assert_classic_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/ml/tests/connect/test_legacy_mode_feature.py`

### Class: `FeatureTests`

#### `test_array_assembler`
Tests ArrayAssembler's ability to concatenate features into a single vector, handle invalid values (NaN/None), and support serialization.

#### `test_max_abs_scaler`
Tests MaxAbsScaler functionality.

#### `test_standard_scaler`
Tests StandardScaler functionality.

#### `test_assert_classic_mode`
*Note: Could not find definition in parity or base files.*

### Class: `ReusedSQLTestCase`

#### `test_assert_classic_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/ml/tests/connect/test_legacy_mode_pipeline.py`

### Class: `PipelineTests`

#### `test_pipeline`
Validates end-to-end pipeline functionality including model fitting, transformation, and state persistence for complex estimator chains.

#### `test_pipeline_copy`
Validates that copying a pipeline with parameters works and doesn't affect the original.

#### `test_assert_classic_mode`
*Note: Could not find definition in parity or base files.*

### Class: `ReusedSQLTestCase`

#### `test_assert_classic_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/ml/tests/connect/test_legacy_mode_summarizer.py`

### Class: `ReusedSQLTestCase`

#### `test_assert_classic_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SummarizerTests`

#### `test_summarize_dataframe`
Tests that summarize_dataframe produces correct results for both Spark DataFrame and local Pandas DataFrame.

#### `test_assert_classic_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/ml/tests/connect/test_legacy_mode_tuning.py`

### Class: `CrossValidatorTests`

#### `test_copy`
Tests that CrossValidator and CrossValidatorModel copy methods work and copy all parameters.

#### `test_crossvalidator_on_pipeline`
Tests CrossValidator with a pipeline, including fitting, transformation accuracy, and save/load (local and torch).

#### `test_crossvalidator_with_fold_col`
Tests CrossValidator with a specific fold column.

#### `test_fit_maximize_metric`
Validates that CrossValidator correctly identifies the model with zero induced error by maximizing the R-squared evaluation metric.

#### `test_fit_minimize_metric`
Validates that CrossValidator correctly identifies the model with zero induced error by minimizing the RMSE evaluation metric.

#### `test_gen_avg_and_std_metrics`
Tests internal helper method _gen_avg_and_std_metrics.

#### `test_assert_classic_mode`
*Note: Could not find definition in parity or base files.*

### Class: `ReusedSQLTestCase`

#### `test_assert_classic_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/ml/tests/connect/test_parity_als.py`

### Class: `ALSParityTests`

#### `test_als`
Tests Alternating Least Squares (ALS) recommendation algorithm.

#### `test_ambiguous_column`
Tests ALS with ambiguous columns handling.

## File: `python/pyspark/ml/tests/connect/test_parity_classification.py`

### Class: `ClassificationParityTests`

#### `test_binary_logistic_regression_summary`
Tests summary for binary logistic regression.

#### `test_binary_random_forest_classifier`
Checks the correctness of Random Forest classification on binary data, confirming valid summary generation, predictions, and model serialization.

#### `test_binomial_logistic_regression_with_bound`
Tests binomial logistic regression with bounds on coefficients and intercepts.

#### `test_decision_tree_classifier`
Tests Decision Tree Classifier.

#### `test_factorization_machine`
Tests Factorization Machine Regressor.

#### `test_gbt_classifier`
Tests Gradient Boosted Trees Classifier.

#### `test_invalid_load_location`
Tests loading from invalid location throws exception.

#### `test_linear_svc`
Tests Linear Support Vector Classification.

#### `test_logistic_regression`
Tests Logistic Regression save/load with bounds.

#### `test_logistic_regression_with_threshold`
Tests Logistic Regression with different thresholds.

#### `test_mlp`
Tests Multilayer Perceptron Classifier.

#### `test_multiclass_logistic_regression_summary`
Tests summary for multiclass logistic regression.

#### `test_multiclass_random_forest_classifier`
Tests Random Forest Classifier for multiclass classification.

#### `test_multinomial_logistic_regression_with_bound`
Tests multinomial logistic regression with bounds.

#### `test_naive_bayes`
Tests Naive Bayes classifier.

## File: `python/pyspark/ml/tests/connect/test_parity_clustering.py`

### Class: `ClusteringParityTests`

#### `test_bisecting_kmeans`
Tests Bisecting K-Means clustering.

#### `test_distributed_lda`
Tests Distributed LDA.

#### `test_gaussian_mixture`
Tests Gaussian Mixture clustering.

#### `test_kmeans`
Validates K-Means clustering centers and consistency, noting the specific constraint that training summaries are inaccessible after model persistence.

#### `test_local_lda`
Tests Local LDA.

#### `test_power_iteration_clustering`
Tests Power Iteration Clustering.

## File: `python/pyspark/ml/tests/connect/test_parity_evaluation.py`

### Class: `EvaluatorParityTests`

#### `test_binary_classification_evaluator`
Verifies metric consistency for binary classification (AUROC, AUPRC) across local Pandas-based computation and Spark Connect.

#### `test_clustering_evaluator`
Tests Clustering Evaluator.

#### `test_clustering_evaluator_with_cosine_distance`
Tests Clustering Evaluator with cosine distance.

#### `test_multiclass_classification_evaluator`
Verifies metric consistency for multiclass classification.

#### `test_multilabel_classification_evaluator`
Tests Multilabel Classification Evaluator.

#### `test_ranking_evaluator`
Tests Ranking Evaluator.

#### `test_regression_evaluator`
Verifies metric consistency for regression.

## File: `python/pyspark/ml/tests/connect/test_parity_feature.py`

### Class: `FeatureParityTests`

#### `test_binarizer`
Tests Binarizer feature transformer.

#### `test_bucketed_random_projection_lsh`
Tests Bucketed Random Projection LSH.

#### `test_bucketizer`
Tests Bucketizer feature transformer.

#### `test_chi_sq_selector`
Tests ChiSqSelector feature selector.

#### `test_count_vectorizer`
Verifies CountVectorizer functionality, including transformation accuracy, vocabulary consistency, and persistence through save/load cycles.

#### `test_count_vectorizer_from_vocab`
Tests creating a CountVectorizerModel from a provided vocabulary list.

#### `test_count_vectorizer_with_binary`
Validates CountVectorizer with binary feature values.

#### `test_count_vectorizer_with_maxDF`
Tests CountVectorizer with maximum document frequency constraints.

#### `test_dct`
Validates Discrete Cosine Transform (DCT) numerical correctness and persistence functionality.

#### `test_elementwise_product`
Tests scaling a vector by another vector element-wise.

#### `test_feature_hasher`
Tests hashing of categorical and numerical features into a feature vector.

#### `test_hashing_tf`
Tests term frequency calculation using the hashing trick.

#### `test_idf`
Confirms IDF model coefficient calculation accuracy, doc frequency logic, and state persistence.

#### `test_imputer`
Tests missing value imputation using mean or median strategies.

#### `test_index_string`
Tests StringIndexer and IndexToString round-trip transformation.

#### `test_interaction`
Tests interaction of multiple columns to produce a vector.

#### `test_maxabs_scaler`
Validates MaxAbsScaler functionality.

#### `test_min_hash_lsh`
Tests Locality Sensitive Hashing for Jaccard distance.

#### `test_minmax_scaler`
Validates MinMaxScaler functionality.

#### `test_ngram`
Tests extracting n-grams from a sequence of strings.

#### `test_normalizer`
Tests normalizing vectors to unit length using a p-norm.

#### `test_one_hot_encoder`
Tests OneHotEncoder functionality.

#### `test_pca`
Tests Principal Component Analysis (PCA) projection accuracy, explained variance calculation, and model persistence.

#### `test_polynomial_expansion`
Tests expanding features into a polynomial space.

#### `test_quantile_discretizer_multiple_columns`
Tests QuantileDiscretizer on multiple columns.

#### `test_quantile_discretizer_single_column`
Tests QuantileDiscretizer on a single column.

#### `test_regex_tokenizer`
Tests RegexTokenizer.

#### `test_rformula_force_index_label`
Tests RFormula with forced label indexing.

#### `test_rformula_string_indexer_order_type`
Tests RFormula with specific string indexer order type.

#### `test_robust_scaler`
Tests RobustScaler.

#### `test_sql_transformer`
Tests SQLTransformer.

#### `test_standard_scaler`
Validates StandardScaler mean/std/scale value calculation and confirms that transform operations do not mutate input dataframes.

#### `test_stop_words_remover`
Tests StopWordsRemover with custom stop words.

#### `test_stop_words_remover_default`
Tests StopWordsRemover default settings.

#### `test_stop_words_remover_with_given_words`
Tests StopWordsRemover with specific words.

#### `test_stop_words_remover_with_turkish`
Tests StopWordsRemover with Turkish locale.

#### `test_string_indexer`
Tests StringIndexer functionality.

#### `test_string_indexer_from_arrays_of_labels`
Tests StringIndexerModel.from_arrays_of_labels.

#### `test_string_indexer_from_labels`
Tests StringIndexerModel.from_labels.

#### `test_string_indexer_handle_invalid`
Tests StringIndexer with invalid values handling.

#### `test_target_encoder_binary`
Tests TargetEncoder for binary target.

#### `test_tokenizer`
Tests Tokenizer.

#### `test_univariate_selector`
Tests UnivariateFeatureSelector.

#### `test_variance_threshold_selector`
Tests VarianceThresholdSelector.

#### `test_vector_assembler`
Tests VectorAssembler functionality including default behavior, custom parameters, save/load, and invalid value handling.

#### `test_vector_indexer`
Tests VectorIndexer fitting, category maps, transforming, and save/load for both estimator and model.

#### `test_vector_size_hint`
Tests VectorSizeHint setting size, transforming, and save/load.

#### `test_vector_slicer`
Tests VectorSlicer slicing by indices, transforming, and save/load.

#### `test_word2vec`
Tests Word2Vec fitting, finding synonyms, transforming, and save/load for estimator and model.

## File: `python/pyspark/ml/tests/connect/test_parity_fpm.py`

### Class: `FPMParityTests`

#### `test_fp_growth`
Tests FPGrowth fitting, frequent itemsets, association rules, transforming, and save/load.

#### `test_prefix_span`
Tests PrefixSpan finding frequent sequential patterns.

## File: `python/pyspark/ml/tests/connect/test_parity_functions.py`

### Class: `ArrayVectorConversionParityTests`

#### `test_array_vector_conversion`
Tests array_to_vector and vector_to_array functions, comparing Connect and classic Spark.

### Class: `PredictBatchUDFParityTests`

#### `test_batching`
Tests batch size enforcement in predict_batch_udf.

#### `test_caching`
Tests caching behavior of predict_batch_udf across calls.

#### `test_identity_multi`
Tests predict_batch_udf with multiple inputs/outputs and error handling.

#### `test_identity_single`
Tests predict_batch_udf with single input/output and different batch sizes.

#### `test_mixed_input_shapes`
Tests predict_batch_udf with a mix of scalar and tensor inputs.

#### `test_return_multiple`
Tests predict_batch_udf returning multiple outputs (dict or list of dicts).

#### `test_return_struct_with_array_field`
Tests predict_batch_udf returning structs with array fields and error handling.

#### `test_single_value_in_batch`
Tests predict_batch_udf with batch size 1 and float values.

#### `test_transform_multi_tensor`
Tests predict_batch_udf with multiple tensor inputs.

#### `test_transform_scalar`
Tests predict_batch_udf with scalar inputs and various parameter combinations/errors.

#### `test_transform_single_tensor`
Tests predict_batch_udf with single tensor input and requires shape.

## File: `python/pyspark/ml/tests/connect/test_parity_ovr.py`

### Class: `OneVsRestParityTests`

#### `test_one_vs_rest`
Tests OneVsRest with LinearSVC, fitting, coefficients, prediction, and save/load.

## File: `python/pyspark/ml/tests/connect/test_parity_pipeline.py`

### Class: `PipelineParityTests`

#### `test_classification_pipeline`
Tests a classification pipeline with save/load.

#### `test_clustering_pipeline`
Tests a clustering pipeline with save/load.

#### `test_identity_pipeline`
Tests empty pipeline and error handling for missing stages.

#### `test_model_attr_df_gc`
Tests garbage collection or union behavior with model attribute dataframes.

#### `test_model_gc`
Tests union behavior with transformed dataframes from models.

#### `test_model_testing_summary_gc`
Tests union behavior with model test summary predictions.

#### `test_model_training_summary_gc`
Tests union behavior with model training summary predictions.

#### `test_pipeline`
Comprehensive test for pipeline with PyTorch, local save/load, and input non-mutation.

## File: `python/pyspark/ml/tests/connect/test_parity_regression.py`

### Class: `RegressionParityTests`

#### `test_aft_survival`
Tests AFTSurvivalRegression with fitting, properties, prediction, and save/load.

#### `test_decision_tree_regressor`
Tests DecisionTreeRegressor with fitting, properties, feature importances, debug string, and save/load.

#### `test_factorization_machine`
Tests FMRegressor with fitting, properties (intercept, linear, factors), prediction, and save/load.

#### `test_gbt_regressor`
Tests GBTRegressor with fitting, properties, trees, and save/load.

#### `test_generalized_linear_regression`
Tests GeneralizedLinearRegression with fitting, properties, summary, and save/load.

#### `test_isotonic_regression`
Tests IsotonicRegression with fitting, properties (boundaries, predictions), prediction, and save/load.

#### `test_linear_regression`
Tests LinearRegression with fitting, properties, summary, and save/load.

#### `test_random_forest_regressor`
Tests RandomForestRegressor with fitting, properties, trees, and save/load.

## File: `python/pyspark/ml/tests/connect/test_parity_stat.py`

### Class: `StatParityTests`

#### `test_chisquaretest`
Tests ChiSquareTest with default and flattened outputs.

#### `test_correlation`
Tests Correlation with Pearson and Spearman methods.

#### `test_illegal_argument`
Tests error handling in ChiSquareTest for invalid field names.

#### `test_kolmogorov_smirnov`
Tests KolmogorovSmirnovTest comparing with expected p-value and statistic.

#### `test_summarizer`
Tests Summarizer computing various metrics with weights.

## File: `python/pyspark/ml/tests/connect/test_parity_tuning.py`

### Class: `TuningParityTests`

#### `test_cross_validator`
Tests CrossValidator with fitting, submodels, save/load, and options.

#### `test_crossvalidator_with_random_forest_classifier`
Basic test for CrossValidator with RandomForestClassifier.

#### `test_cv_invalid_user_specified_folds`
Tests error handling in CrossValidator for invalid folds or empty validation data.

#### `test_cv_user_specified_folds`
Tests CrossValidator using a specific column for folds.

#### `test_train_validation_split`
Tests TrainValidationSplit with fitting, metrics, and save/load.

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_any_all.py`

### Class: `FrameParityAnyAllTests`

#### `test_all`
Tests all() method on pandas-on-Spark DataFrames, comparing with pandas.

#### `test_any`
Tests any() method on pandas-on-Spark DataFrames, comparing with pandas.

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_apply_func.py`

### Class: `FrameParityApplyFunctionTests`

#### `test_aggregate`
Tests agg() on pandas-on-Spark GroupBy, comparing with pandas.

#### `test_apply`
Tests apply() on pandas-on-Spark GroupBy, comparing with pandas (handles pandas 3 changes).

#### `test_apply_batch`
Tests pandas_on_spark.apply_batch() on DataFrames.

#### `test_apply_batch_with_type`
Tests pandas_on_spark.apply_batch() with type hints.

#### `test_apply_with_type`
Tests apply() with type hints on DataFrames.

#### `test_pipe`
Tests pipe() on DataFrames and error handling.

#### `test_transform`
Tests transform() on Spark Connect Columns with built-in and lambda functions.

#### `test_transform_batch`
Tests pandas_on_spark.transform_batch() on DataFrames.

#### `test_transform_batch_same_anchor`
Tests transform_batch() returning Series and assigning back.

#### `test_transform_batch_with_type`
Tests transform_batch() with type hints.

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_binary_ops.py`

### Class: `FrameParityBinaryOpsTests`

#### `test_binary_operator_add`
Tests + operator on pandas-on-Spark Series and error handling.

#### `test_binary_operator_floordiv`
Tests // operator on pandas-on-Spark Series and error handling.

#### `test_binary_operator_mod`
Tests % operator on pandas-on-Spark Series and error handling.

#### `test_binary_operator_multiply`
Tests * operator on pandas-on-Spark Series including string replication, and error handling.

#### `test_binary_operator_sub`
Tests - operator on pandas-on-Spark Series and error handling.

#### `test_binary_operator_truediv`
Tests / operator on pandas-on-Spark Series and error handling.

#### `test_binary_operators`
Tests operations between different frames and error handling.

#### `test_combine_first`
Tests combine_first() on pandas-on-Spark Series.

#### `test_divide_by_zero_behavior`
Tests behavior of division by zero for various dtypes in pandas-on-Spark.

#### `test_dot`
Tests dot() method on ML Vectors with vectors and arrays.

#### `test_mixed_dataframe_ops_dispatch_to_pandas_on_spark`
Tests that mixed operations between pandas and pandas-on-Spark frames are disallowed.

#### `test_rfloordiv`
Tests rfloordiv() on pandas-on-Spark DataFrames.

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_combine.py`

### Class: `FrameParityCombineTests`

#### `test_concat`
Tests ps.concat() with various parameters (ignore_index, sort, multi-index).

#### `test_join`
Tests join() on pandas-on-Spark DataFrames with various parameters.

#### `test_merge`
Tests merge() on pandas-on-Spark DataFrames with various parameters.

#### `test_merge_cross`
Tests cross join in merge().

#### `test_merge_cross_raises`
Tests error handling for cross join with invalid parameters.

#### `test_merge_how_parameter`
Tests different join types in merge().

#### `test_merge_raises`
Tests error handling in merge() for missing common columns, mismatched keys, etc.

#### `test_merge_retains_indices`
Tests that merge() retains indices when using left_index or right_index.

#### `test_merge_same_anchor`
Tests merge() when both sides share the same anchor.

#### `test_update`
Tests update() on pandas-on-Spark DataFrames with various parameters.

#### `test_update_errors_invalid_value`
Tests error handling for invalid errors parameter in update().

#### `test_update_errors_raise_no_overlap`
Tests update() with errors='raise' when no data overlap.

#### `test_update_errors_raise_with_overlap`
Tests update() with errors='raise' raises error when data overlap.

#### `test_update_filter_func_all_false`
Tests update() with filter_func that returns all False.

#### `test_update_filter_func_and_errors_raise`
Tests combination of filter_func and errors='raise' in update().

#### `test_update_filter_func_overwrite_false`
Tests filter_func with overwrite=False in update().

#### `test_update_filter_func_with_nulls`
Tests filter_func handling of null values in update().

#### `test_update_with_filter_func`
Tests filter_func parameter in update().

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_compute.py`

### Class: `FrameParityComputeTests`

#### `test_abs`
Tests abs() on pandas-on-Spark Index objects.

#### `test_clip`
Tests clip() on pandas-on-Spark DataFrames.

#### `test_diff`
Tests diff() on pandas-on-Spark GroupBy objects.

#### `test_mode`
Tests mode() on pandas-on-Spark DataFrames, including a mapped dataframe.

#### `test_nunique`
Tests nunique() on pandas-on-Spark GroupBy objects.

#### `test_nunique_with_string_column_and_missing_values`
Tests nunique() on pandas-on-Spark DataFrames with string columns and missing values.

#### `test_pct_change`
Tests pct_change() on pandas-on-Spark DataFrames.

#### `test_product`
Tests product() on pandas-on-Spark DataFrames and Series.

#### `test_quantile`
Tests quantile() on pandas-on-Spark GroupBy objects.

#### `test_rank`
Tests rank() on pandas-on-Spark GroupBy objects.

#### `test_rank_axis`
Tests basic axis parameter functionality in rank() on pandas-on-Spark DataFrames.

#### `test_round`
Tests round() on pandas-on-Spark DatetimeIndex objects.

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_corr.py`

### Class: `FrameParityCorrTests`

#### `test_cov_corr_meta`
Tests correlation matrix calculation on a pandas-on-Spark DataFrame with various data types, ensuring it matches Pandas' correlation with `numeric_only=True` while Arrow execution is disabled due to UDT limitations.

#### `test_dataframe_corr`
Verifies DataFrame correlation (`corr`) across 'pearson', 'spearman', and 'kendall' methods, testing edge cases like invalid arguments, minimum periods, multi-index columns, and DataFrames with identical or constant values.

#### `test_series_corr`
Validates correlation between two Series (`corr`) using different methods and minimum period settings, including tests for operations on different anchors and testing error raising for invalid methods.

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_corrwith.py`

### Class: `FrameParityCorrwithTests`

#### `test_corrwith`
Tests the `corrwith` method between DataFrames and Series/DataFrames, checking error conditions for unsupported axes and methods, and operations on boolean data.

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_cov.py`

### Class: `FrameParityCovTests`

#### `test_cov`
Tests sample covariance calculation between two columns using Spark DataFrame's `stat.cov` API and ensures appropriate PySparkTypeError is raised for invalid column types.

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_cumulative.py`

### Class: `FrameParityCumulativeTests`

#### `test_cummax`
Verifies `cummax` (cumulative maximum) execution after `groupby` under various configurations: grouped by single/multiple columns, with renamed columns, multi-index columns, and proper handling of invalid operations raising DataError.

#### `test_cummax_multiindex_columns`
Focuses on testing `cummax` functionality specifically for DataFrames with MultiIndex columns, comparing results against Pandas.

#### `test_cummin`
Verifies `cummin` (cumulative minimum) execution after `groupby` under various configurations: grouped by single/multiple columns, with renamed columns, multi-index columns, and checks proper DataError handling for invalid input DataFrames.

#### `test_cummin_multiindex_columns`
Specifically tests `cummin` functionality on DataFrames containing MultiIndex columns against Pandas equivalent operations.

#### `test_cumprod`
Verifies `cumprod` (cumulative product) execution after `groupby` across combinations: single/multiple grouping columns, renamed columns, multi-index columns, and properly catching DataError on incompatible types.

#### `test_cumprod_multiindex_columns`
Dedicated test validating `cumprod` computation correctness for DataFrames using MultiIndex columns.

#### `test_cumsum`
Verifies `cumsum` (cumulative sum) execution after `groupby` under various configurations: single/multiple columns, with renamed columns, multi-index columns, and verifies proper DataError triggers for unsupported data types.

#### `test_cumsum_multiindex_columns`
Specific verification of the `cumsum` behavior correct execution against DataFrames populated with MultiIndex columns.

#### `test_cumulative_reduction_preserves_none_name`
Asserts that cumulative reduction like `cumsum().sum()` preserves the default `None` value for the Pandas name property across both Pandas and pandas-on-Spark implementations.

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_describe.py`

### Class: `FrameParityDescribeTests`

#### `test_describe`
Checks the generated plan structure for the `describe` operator in Spark Connect, ensuring expected columns list properties match specification filters.

#### `test_describe_empty`
Tests `describe` behavior when DataFrames are empty, specifically covering edge cases for string, timestamp, numeric mixes, and handling of Pandas versions before and after 3.0.0.

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_eval.py`

### Class: `FrameParityEvalTests`

#### `test_eval`
Exercises Python code evaluation on DataFrames (`eval`) for simple arithmetic, assignments, inplace updates, and validates standard TypeError boundaries like MultiIndex columns blocking execution.

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_idxmax_idxmin.py`

### Class: `FrameParityIdxMaxMinTests`

#### `test_idxmax`
Tests `idxmax` execution downstream of groupby operations with multiple combinations of indices and data types, including checks against DataFrame MultiIndex column limitations.

#### `test_idxmax_empty_dataframe`
Tests `idxmax` boundary condition behavior when applied to empty DataFrames along axis=1, expecting appropriate ValueErrors.

#### `test_idxmax_multiindex_columns`
Confirms that MultiIndex columns raise the anticipated NotImplementedError when calculating `idxmax` specifying axis=1.

#### `test_idxmin`
Tests `idxmin` execution downstream of groupby operations with multiple combinations of indices and data types, including check blocks against DataFrame MultiIndex column operations.

#### `test_idxmin_multiindex_columns`
Confirms that MultiIndex columns raise the anticipated NotImplementedError when calculating `idxmin` specifying axis=1.

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_melt.py`

### Class: `FrameParityMeltTests`

#### `test_melt`
Checks the generated plan structure for the `melt` operator in Spark Connect, ensuring the `unpivot` identifiers and values align with the passed argument lists.

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_missing_data.py`

### Class: `FrameParityMissingDataTests`

#### `test_backfill`
Tests the `backfill` method for filling missing values, validating both out-of-place and in-place applications, and ensuring AttributeError is raised when executed against Pandas 3.0.0 or higher.

#### `test_bfill`
Validates `bfill` operation correct execution chained after `groupby` aggregators across DataFrames containing single or MultiIndex columns.

#### `test_dropna_axis_column`
Tests `dropna` behavior specifically focused on operations across column axis (axis=1), checking edge cases like boundary lengths and empty frames.

#### `test_dropna_axis_index`
Asserts `dropna` behavior specifically focused on operations across standard index axis (axis=0), checking empty states, entirely NA frames, and proper parameter validation errors.

#### `test_ffill`
Validates `ffill` operation correct execution chained after `groupby` aggregators across DataFrames containing single or MultiIndex columns.

#### `test_fillna`
Tests DataFrame `fillna` behavior across numeric, boolean, and string values specifying sub-scopes and dictionary lookups, matching correct PySparkTypeError handling.

#### `test_pad`
Tests the `pad` method for filling missing values, validating both out-of-place and in-place applications, and ensuring AttributeError triggers when executed against Pandas 3.0.0 or higher.

#### `test_replace`
Checks the generated plan structure for the `replace` (or `na.replace`) operations in Spark Connect, ensuring target replacement matches expectations.

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_pivot.py`

### Class: `FrameParityPivotTests`

#### `test_pivot_errors`
Asserts DataFrames raise necessary ValueErrors if arguments to `pivot` fail to resolve accurately.

#### `test_pivot_table_and_index`
Tests execution of complex dataframe pivot tables by comparing aggregations computed between both Pandas and Pandas-on-Spark frames.

#### `test_pivot_table_dtypes`
Validates that data types resulting from pivot table computations match correctly against the source frame.

#### `test_pivot_table_errors`
Tests DataFrame input edge cases on `pivot_table` execution that fail, and verifies expected strings match return error messages.

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_pivot_table.py`

### Class: `PivotTableParityTests`

#### `test_pivot_table`
Tests execution of complex dataframe pivot tables checking sums, multi-column averages, and behavior of the `fill_value` fill property.

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_pivot_table_adv.py`

### Class: `PivotTableAdvParityTests`

#### `test_pivot_table`
Repeated checks mirroring Test 36, likely validating the same functionality under a different file scope.

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_pivot_table_multi_idx.py`

### Class: `PivotTableMultiIdxParityTests`

#### `test_pivot_table`
Repeated checks mirroring Test 36, likely validating the same functionality under a different file scope.

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_pivot_table_multi_idx_adv.py`

### Class: `PivotTableMultiIdxAdvParityTests`

#### `test_pivot_table`
Repeated checks mirroring Test 36, likely validating the same functionality under a different file scope.

## File: `python/pyspark/pandas/tests/connect/computation/test_parity_stats.py`

### Class: `StatsParityTests`

#### `test_abs`
Tests the behavior of `abs` (absolute value) operation on a Pandas-on-Spark Index against standard Pandas index behavior.

#### `test_numeric_only_unsupported`
Checks that `sum(numeric_only=True)` correctly resolves while non-numeric strings trigger the expected conversions failures.

#### `test_product`
Validates `product` (multiplication product) boundary cases over columns and index subsets specifying missing value min threshold blocks.

#### `test_skew_kurt_numerical_stability`
Tests both skew and kurtosis calculations for accuracy tolerances on boundary frames.

#### `test_stat_functions`
Verifies an extensive list of statistical functions like sum, mean, min, max, std, var, sem, and skewness for boundary conditions and edge states.

#### `test_stat_functions_multiindex_column`
Executes Test 44 functionality checks but tailored to verify against frames containing active MultiIndex columns.

#### `test_stat_functions_with_no_numeric_columns`
Executes Test 44 checks over DataFrames completely populated by non-numeric values.

#### `test_stats_on_boolean_dataframe`
Tests DataFrame execution of min, max, count, sum, mean, var, std, and sem over frames containing boolean values.

#### `test_stats_on_boolean_series`
Tests Series execution of min, max, count, sum, mean, var, std, and sem over Series containing boolean values.

#### `test_stats_on_non_numeric_columns_should_be_discarded_if_numeric_only_is_true`
Verifies that operations on non-numeric columns are discarded when specified by setting `numeric_only=True` parameter.

#### `test_sum`
Tests standard sum operations under multiple grouping conditions specifying minimal count threshold properties.

## File: `python/pyspark/pandas/tests/connect/data_type_ops/test_parity_as_type.py`

### Class: `AsTypeParityTests`

#### `test_astype`
Tests `astype` executions against target category conversions and string conversions.

#### `test_astype_eager_check`
Tests expected ValueError failures in cases of `compute.eager_check` option configuration active.

## File: `python/pyspark/pandas/tests/connect/data_type_ops/test_parity_base.py`

### Class: `BaseParityTests`

#### `test_bool_ext_ops`
Verifies instantiation of target operation blocks depending on whether extension object dtypes are available.

#### `test_data_type_ops`
Confirms that DataTypeOps returns proper mapped operations classes mapped against the requested dataframe execution types.

## File: `python/pyspark/pandas/tests/connect/data_type_ops/test_parity_binary_ops.py`

### Class: `BinaryOpsParityTests`

#### `test_abs`
Tests absolute value `abs` function execution over standard Index objects.

#### `test_add`
Tests array addition operations for numeric arrays and confirms that mixed type addition returns appropriate TypeErrors.

#### `test_and`
Tests boolean bitwise AND (`&`) operator behavior on Series and with scalar boolean values.

#### `test_astype`
Repeated checks mirroring Test 51.

#### `test_eq`
Tests linear algebra equality computations (`==`) for vectors and matrices.

#### `test_floordiv`
Confirms floor division (`//`) blocks execution when invoked over incompatible types, raising TypeErrors.

#### `test_from_to_pandas`
Tests two-way conversion correctness between Pandas and Pandas-on-Spark frames for array data.

#### `test_ge`
Tests greater-than-or-equal (`>=`) comparison behavior for arrays and structures.

#### `test_gt`
Tests greater-than (`>`) comparison behavior for arrays and structures.

#### `test_invert`
Validates that bitwise NOT (`~`) operator blocks execution when invoked on complex types.

#### `test_isnull`
Checks correct execution for DataFrame `notnull()` and `isnull()` operations.

#### `test_le`
Tests less-than-or-equal (`<=`) comparison behavior for arrays and structures.

#### `test_lt`
Tests less-than (`<`) comparison behavior for arrays and structures.

#### `test_mod`
Checks modulo (`%`) computation correctness between different series objects.

#### `test_mul`
Validates that multiplication (`*`) triggers expected TypeErrors on unsupported type arguments.

#### `test_ne`
Tests inequality (`!=`) comparison behavior for arrays and structures.

#### `test_neg`
Confirms negation (`-`) triggers expected TypeErrors when executed on complex data.

#### `test_or`
Tests boolean bitwise OR (`|`) operator behavior on Series and with scalar boolean values.

#### `test_pow`
Confirms that power (`**`) operator raises TypeError when applied to Series and incompatible types.

#### `test_radd`
Confirms that right addition (`+`) with a string on the left raises a TypeError.

#### `test_rand`
Confirms that right bitwise AND (`&`) with a string on the left raises a TypeError.

#### `test_rfloordiv`
Confirms that right floor division (`//`) with a string on the left raises a TypeError.

#### `test_rmod`
Confirms that right modulo (`%`) with a string on the left raises a TypeError.

#### `test_rmul`
Confirms that right multiplication (`*`) with a string on the left raises a TypeError.

#### `test_ror`
Confirms that right bitwise OR (`|`) with a string on the left raises a TypeError.

#### `test_rpow`
Confirms that right power (`**`) with a string on the left raises a TypeError.

#### `test_rsub`
Confirms that right subtraction (`-`) with a string or integer on the left raises a TypeError.

#### `test_rtruediv`
Confirms that right true division (`/`) with a string or integer on the left raises a TypeError.

#### `test_sub`
Tests that subtraction (`-`) raises TypeError when applied to Series and incompatible types or between DataFrame columns.

#### `test_truediv`
Tests that true division (`/`) raises TypeError when applied to Series and incompatible types or between DataFrame columns.

## File: `python/pyspark/pandas/tests/connect/data_type_ops/test_parity_boolean_ops.py`

### Class: `BooleanExtensionOpsParityTests`

#### `test_abs`
Verifies that the absolute value (`abs`) operation is not supported for BooleanExtensionOpsParityTests and raises a TypeError.

#### `test_add`
Verifies that the addition (`+`) operation is not supported for BooleanExtensionOpsParityTests and raises a TypeError.

#### `test_and`
Verifies that the bitwise AND (`&`) operation on BooleanExtensionOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_astype`
Verifies that the type casting (`astype`) operation on BooleanExtensionOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_eq`
Verifies that the equality (`==`) operation on BooleanExtensionOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_floordiv`
Verifies that the floor division (`//`) operation is not supported for BooleanExtensionOpsParityTests and raises a TypeError.

#### `test_from_to_pandas`
Verifies that the conversion to/from pandas operation on BooleanExtensionOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_ge`
Verifies that the greater than or equal (`>=`) operation on BooleanExtensionOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_gt`
Verifies that the greater than (`>`) operation on BooleanExtensionOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_invert`
Verifies that the bitwise invert (`~`) operation is not supported for BooleanExtensionOpsParityTests and raises a TypeError.

#### `test_isnull`
Verifies that the null check (`isnull` / `notnull`) operation on BooleanExtensionOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_le`
Verifies that the less than or equal (`<=`) operation on BooleanExtensionOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_lt`
Verifies that the less than (`<`) operation on BooleanExtensionOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_mod`
Verifies that the modulo (`%`) operation on BooleanExtensionOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_mul`
Verifies that the multiplication (`*`) operation is not supported for BooleanExtensionOpsParityTests and raises a TypeError.

#### `test_ne`
Verifies that the not equal (`!=`) operation on BooleanExtensionOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_neg`
Verifies that the negation (`-`) operation is not supported for BooleanExtensionOpsParityTests and raises a TypeError.

#### `test_or`
Verifies that the bitwise OR (`|`) operation on BooleanExtensionOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_pow`
Verifies that the power (`**`) operation is not supported for BooleanExtensionOpsParityTests and raises a TypeError.

#### `test_radd`
Verifies that the right addition (`+`) operation is not supported for BooleanExtensionOpsParityTests and raises a TypeError.

#### `test_rand`
Verifies that the right bitwise AND (`&`) operation is not supported for BooleanExtensionOpsParityTests and raises a TypeError.

#### `test_rfloordiv`
Verifies that the right floor division (`//`) operation on BooleanExtensionOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_rmod`
Verifies that the right modulo (`%`) operation on BooleanExtensionOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_rmul`
Verifies that the right multiplication (`*`) operation is not supported for BooleanExtensionOpsParityTests and raises a TypeError.

#### `test_ror`
Verifies that the right bitwise OR (`|`) operation is not supported for BooleanExtensionOpsParityTests and raises a TypeError.

#### `test_rpow`
Verifies that the right power (`**`) operation is not supported for BooleanExtensionOpsParityTests and raises a TypeError.

#### `test_rsub`
Verifies that the right subtraction (`-`) operation is not supported for BooleanExtensionOpsParityTests and raises a TypeError.

#### `test_rtruediv`
Verifies that the right true division (`/`) operation is not supported for BooleanExtensionOpsParityTests and raises a TypeError.

#### `test_rxor`
Verifies that the right bitwise XOR (`^`) operation is not supported for BooleanExtensionOpsParityTests and raises a TypeError.

#### `test_sub`
Verifies that the subtraction (`-`) operation is not supported for BooleanExtensionOpsParityTests and raises a TypeError.

#### `test_truediv`
Verifies that the true division (`/`) operation is not supported for BooleanExtensionOpsParityTests and raises a TypeError.

#### `test_xor`
Verifies that the bitwise XOR (`^`) operation is not supported for BooleanExtensionOpsParityTests and raises a TypeError.

### Class: `BooleanOpsParityTests`

#### `test_abs`
Repeated checks mirroring Test 40 and 55, testing `abs` on Index.

#### `test_add`
Repeated checks mirroring Test 56, testing array addition.

#### `test_and`
Repeated checks mirroring Test 57, testing bitwise AND.

#### `test_astype`
Repeated checks mirroring Test 51 and 58, testing `astype`.

#### `test_eq`
Repeated checks mirroring Test 59, testing linear algebra equality.

#### `test_floordiv`
Repeated checks mirroring Test 60, testing floor division failures.

#### `test_ge`
Repeated checks mirroring Test 62, testing `>=` comparison.

#### `test_gt`
Repeated checks mirroring Test 63, testing `>` comparison.

#### `test_invert`
Repeated checks mirroring Test 64, testing bitwise NOT failures.

#### `test_isnull`
Repeated checks mirroring Test 65, testing `isnull` and `notnull`.

#### `test_le`
Repeated checks mirroring Test 66, testing `<=` comparison.

#### `test_lt`
Repeated checks mirroring Test 67, testing `<` comparison.

#### `test_mod`
Repeated checks mirroring Test 68, testing modulo computation.

#### `test_mul`
Repeated checks mirroring Test 69, testing multiplication failures.

#### `test_ne`
Repeated checks mirroring Test 70, testing inequality comparison.

#### `test_neg`
Repeated checks mirroring Test 71, testing negation failures.

#### `test_or`
Verifies that the bitwise OR (`|`) operation on BooleanOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_pow`
Verifies that the power (`**`) operation is not supported for BooleanOpsParityTests and raises a TypeError.

#### `test_radd`
Verifies that the right addition (`+`) operation is not supported for BooleanOpsParityTests and raises a TypeError.

#### `test_rand`
Verifies that the right bitwise AND (`&`) operation is not supported for BooleanOpsParityTests and raises a TypeError.

#### `test_rfloordiv`
Verifies that the right floor division (`//`) operation on BooleanOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_rmod`
Verifies that the right modulo (`%`) operation on BooleanOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_rmul`
Verifies that the right multiplication (`*`) operation is not supported for BooleanOpsParityTests and raises a TypeError.

#### `test_ror`
Verifies that the right bitwise OR (`|`) operation is not supported for BooleanOpsParityTests and raises a TypeError.

#### `test_rpow`
Verifies that the right power (`**`) operation is not supported for BooleanOpsParityTests and raises a TypeError.

#### `test_rsub`
Verifies that the right subtraction (`-`) operation is not supported for BooleanOpsParityTests and raises a TypeError.

#### `test_rtruediv`
Verifies that the right true division (`/`) operation is not supported for BooleanOpsParityTests and raises a TypeError.

#### `test_rxor`
Verifies that the right bitwise XOR (`^`) operation is not supported for BooleanOpsParityTests and raises a TypeError.

#### `test_sub`
Verifies that the subtraction (`-`) operation is not supported for BooleanOpsParityTests and raises a TypeError.

#### `test_truediv`
Verifies that the true division (`/`) operation is not supported for BooleanOpsParityTests and raises a TypeError.

#### `test_xor`
Verifies that the bitwise XOR (`^`) operation is not supported for BooleanOpsParityTests and raises a TypeError.

## File: `python/pyspark/pandas/tests/connect/data_type_ops/test_parity_categorical_ops.py`

### Class: `CategoricalOpsParityTests`

#### `test_abs`
Verifies that the absolute value (`abs`) operation is not supported for CategoricalOpsParityTests and raises a TypeError.

#### `test_add`
Verifies that the addition (`+`) operation is not supported for CategoricalOpsParityTests and raises a TypeError.

#### `test_and`
Verifies that the bitwise AND (`&`) operation on CategoricalOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_astype`
Verifies that the type casting (`astype`) operation on CategoricalOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_eq`
Verifies that the equality (`==`) operation on CategoricalOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_floordiv`
Verifies that the floor division (`//`) operation is not supported for CategoricalOpsParityTests and raises a TypeError.

#### `test_from_to_pandas`
Verifies that the conversion to/from pandas operation on CategoricalOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_ge`
Verifies that the greater than or equal (`>=`) operation on CategoricalOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_gt`
Verifies that the greater than (`>`) operation on CategoricalOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_invert`
Verifies that the bitwise invert (`~`) operation is not supported for CategoricalOpsParityTests and raises a TypeError.

#### `test_isnull`
Verifies that the null check (`isnull` / `notnull`) operation on CategoricalOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_le`
Verifies that the less than or equal (`<=`) operation on CategoricalOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_lt`
Verifies that the less than (`<`) operation on CategoricalOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_mod`
Verifies that the modulo (`%`) operation on CategoricalOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_mul`
Verifies that the multiplication (`*`) operation is not supported for CategoricalOpsParityTests and raises a TypeError.

#### `test_ne`
Verifies that the not equal (`!=`) operation on CategoricalOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_neg`
Verifies that the negation (`-`) operation is not supported for CategoricalOpsParityTests and raises a TypeError.

#### `test_or`
Verifies that the bitwise OR (`|`) operation on CategoricalOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_pow`
Verifies that the power (`**`) operation is not supported for CategoricalOpsParityTests and raises a TypeError.

#### `test_radd`
Verifies that the right addition (`+`) operation is not supported for CategoricalOpsParityTests and raises a TypeError.

#### `test_rand`
Verifies that the right bitwise AND (`&`) operation is not supported for CategoricalOpsParityTests and raises a TypeError.

#### `test_rfloordiv`
Verifies that the right floor division (`//`) operation on CategoricalOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_rmod`
Verifies that the right modulo (`%`) operation on CategoricalOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_rmul`
Verifies that the right multiplication (`*`) operation is not supported for CategoricalOpsParityTests and raises a TypeError.

#### `test_ror`
Verifies that the right bitwise OR (`|`) operation is not supported for CategoricalOpsParityTests and raises a TypeError.

#### `test_rpow`
Verifies that the right power (`**`) operation is not supported for CategoricalOpsParityTests and raises a TypeError.

#### `test_rsub`
Verifies that the right subtraction (`-`) operation is not supported for CategoricalOpsParityTests and raises a TypeError.

#### `test_rtruediv`
Verifies that the right true division (`/`) operation is not supported for CategoricalOpsParityTests and raises a TypeError.

#### `test_sub`
Verifies that the subtraction (`-`) operation is not supported for CategoricalOpsParityTests and raises a TypeError.

#### `test_truediv`
Verifies that the true division (`/`) operation is not supported for CategoricalOpsParityTests and raises a TypeError.

## File: `python/pyspark/pandas/tests/connect/data_type_ops/test_parity_complex_ops.py`

### Class: `ComplexOpsParityTests`

#### `test_abs`
Verifies that the absolute value (`abs`) operation is not supported for ComplexOpsParityTests and raises a TypeError.

#### `test_add`
Verifies that the addition (`+`) operation is not supported for ComplexOpsParityTests and raises a TypeError.

#### `test_and`
Verifies that the bitwise AND (`&`) operation on ComplexOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_astype`
Verifies that the type casting (`astype`) operation on ComplexOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_eq`
Verifies that the equality (`==`) operation on ComplexOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_floordiv`
Verifies that the floor division (`//`) operation is not supported for ComplexOpsParityTests and raises a TypeError.

#### `test_from_to_pandas`
Verifies that the conversion to/from pandas operation on ComplexOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_ge`
Verifies that the greater than or equal (`>=`) operation on ComplexOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_gt`
Verifies that the greater than (`>`) operation on ComplexOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_invert`
Verifies that the bitwise invert (`~`) operation is not supported for ComplexOpsParityTests and raises a TypeError.

#### `test_isnull`
Verifies that the null check (`isnull` / `notnull`) operation on ComplexOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_le`
Verifies that the less than or equal (`<=`) operation on ComplexOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_lt`
Verifies that the less than (`<`) operation on ComplexOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_mod`
Verifies that the modulo (`%`) operation on ComplexOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_mul`
Verifies that the multiplication (`*`) operation is not supported for ComplexOpsParityTests and raises a TypeError.

#### `test_ne`
Verifies that the not equal (`!=`) operation on ComplexOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_neg`
Verifies that the negation (`-`) operation is not supported for ComplexOpsParityTests and raises a TypeError.

#### `test_or`
Verifies that the bitwise OR (`|`) operation on ComplexOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_pow`
Verifies that the power (`**`) operation is not supported for ComplexOpsParityTests and raises a TypeError.

#### `test_radd`
Verifies that the right addition (`+`) operation is not supported for ComplexOpsParityTests and raises a TypeError.

#### `test_rand`
Verifies that the right bitwise AND (`&`) operation is not supported for ComplexOpsParityTests and raises a TypeError.

#### `test_rfloordiv`
Verifies that the right floor division (`//`) operation on ComplexOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_rmod`
Verifies that the right modulo (`%`) operation on ComplexOpsParityTests matches the behavior of pandas or expected behavior.

#### `test_rmul`
Tests that right-multiplication (__rmul__) of a pandas-on-Spark series with complex types by a string or an integer raises a TypeError.

#### `test_ror`
Tests that right-bitwise-OR (__ror__) of a pandas-on-Spark series with complex types by a boolean value raises a TypeError.

#### `test_rpow`
Tests that right-exponentiation (__rpow__) of a pandas-on-Spark series with complex types by a string or an integer raises a TypeError.

#### `test_rsub`
Tests that right-subtraction (__rsub__) of a pandas-on-Spark series with complex types from a string or an integer raises a TypeError.

#### `test_rtruediv`
Tests that right-division (__rtruediv__) of a pandas-on-Spark series with complex types into a string or an integer raises a TypeError.

#### `test_sub`
Tests that subtraction (__sub__) of a string, an integer, or another complex type series from a pandas-on-Spark series with complex types raises a TypeError.

#### `test_truediv`
Tests that true division operations on complex data types raise TypeError.

## File: `python/pyspark/pandas/tests/connect/data_type_ops/test_parity_date_ops.py`

### Class: `DateOpsParityTests`

#### `test_abs`
Tests that taking the absolute value of a pandas-on-Spark index matches pandas, and that it raises an error for MultiIndex.

#### `test_add`
Tests addition operations for date operations, checking behavior for numeric array + numeric array, non-numeric + non-numeric, and numeric + non-numeric.

#### `test_and`
Tests bitwise AND operations between boolean series and with scalar values.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_astype`
Tests casting a string series to category and related operations.

#### `test_eq`
Tests equality comparisons between various dense and sparse vectors and matrices.

#### `test_floordiv`
Tests that floor division operations on complex data types raise TypeError.

#### `test_from_to_pandas`
Tests that converting a pandas-on-Spark series to pandas and back yields the same result.

#### `test_ge`
Tests greater-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_gt`
Tests greater-than comparisons between complex data types (arrays and structs).

#### `test_invert`
Tests that the bitwise NOT (invert) operator raises TypeError on this data type.

#### `test_isnull`
Tests notnull and isnull methods on a DataFrame.

#### `test_le`
Tests less-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_lt`
Tests less-than comparisons between complex data types (arrays and structs).

#### `test_mod`
Tests modulo operations between two series, sorting the index before comparison.

#### `test_mul`
Tests that multiplication operations on complex data types raise TypeError.

#### `test_ne`
Tests not-equal comparisons between complex data types (arrays and structs).

#### `test_neg`
Tests that the negation operator raises TypeError on this data type.

#### `test_or`
Tests bitwise OR operations between boolean series and with scalar values.

#### `test_pow`
Tests that exponentiation operations on complex data types raise TypeError.

#### `test_radd`
Tests that reflected addition raises TypeError.

#### `test_rand`
Tests that reflected bitwise AND raises TypeError.

#### `test_rfloordiv`
Tests reflected floor division with a scalar value.

#### `test_rmod`
Tests reflected modulo operations between two series.

#### `test_rmul`
Tests that reflected multiplication raises TypeError.

#### `test_ror`
Tests that reflected bitwise OR raises TypeError.

#### `test_rpow`
Tests that reflected exponentiation raises TypeError.

#### `test_rsub`
Tests that reflected subtraction raises TypeError.

#### `test_rtruediv`
Tests that reflected true division raises TypeError.

#### `test_sub`
Tests that subtraction operations on complex data types raise TypeError.

#### `test_truediv`
Tests that true division operations on complex data types raise TypeError.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/data_type_ops/test_parity_datetime_ops.py`

### Class: `DatetimeNTZOpsParityTests`

#### `test_abs`
Tests that taking the absolute value of a pandas-on-Spark index matches pandas, and that it raises an error for MultiIndex.

#### `test_add`
Tests addition operations for datetime operations, checking behavior for numeric array + numeric array, non-numeric + non-numeric, and numeric + non-numeric.

#### `test_and`
Tests bitwise AND operations between boolean series and with scalar values.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_astype`
Tests casting a string series to category and related operations.

#### `test_eq`
Tests equality comparisons between various dense and sparse vectors and matrices.

#### `test_floordiv`
Tests that floor division operations on complex data types raise TypeError.

#### `test_from_to_pandas`
Tests that converting a pandas-on-Spark series to pandas and back yields the same result.

#### `test_ge`
Tests greater-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_gt`
Tests greater-than comparisons between complex data types (arrays and structs).

#### `test_invert`
Tests that the bitwise NOT (invert) operator raises TypeError on this data type.

#### `test_isnull`
Tests notnull and isnull methods on a DataFrame.

#### `test_le`
Tests less-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_lt`
Tests less-than comparisons between complex data types (arrays and structs).

#### `test_mod`
Tests modulo operations between two series, sorting the index before comparison.

#### `test_mul`
Tests that multiplication operations on complex data types raise TypeError.

#### `test_ne`
Tests not-equal comparisons between complex data types (arrays and structs).

#### `test_neg`
Tests that the negation operator raises TypeError on this data type.

#### `test_or`
Tests bitwise OR operations between boolean series and with scalar values.

#### `test_pow`
Tests that exponentiation operations on complex data types raise TypeError.

#### `test_radd`
Tests that reflected addition raises TypeError.

#### `test_rand`
Tests that reflected bitwise AND raises TypeError.

#### `test_rfloordiv`
Tests reflected floor division with a scalar value.

#### `test_rmod`
Tests reflected modulo operations between two series.

#### `test_rmul`
Tests that reflected multiplication raises TypeError.

#### `test_ror`
Tests that reflected bitwise OR raises TypeError.

#### `test_rpow`
Tests that reflected exponentiation raises TypeError.

#### `test_rsub`
Tests that reflected subtraction raises TypeError.

#### `test_rtruediv`
Tests that reflected true division raises TypeError.

#### `test_sub`
Tests that subtraction operations on complex data types raise TypeError.

#### `test_truediv`
Tests that true division operations on complex data types raise TypeError.

### Class: `DatetimeOpsParityTests`

#### `test_abs`
Tests that taking the absolute value of a pandas-on-Spark index matches pandas, and that it raises an error for MultiIndex.

#### `test_add`
Tests addition operations for datetime operations, checking behavior for numeric array + numeric array, non-numeric + non-numeric, and numeric + non-numeric.

#### `test_and`
Tests bitwise AND operations between boolean series and with scalar values.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_astype`
Tests casting a string series to category and related operations.

#### `test_eq`
Tests equality comparisons between various dense and sparse vectors and matrices.

#### `test_floordiv`
Tests that floor division operations on complex data types raise TypeError.

#### `test_from_to_pandas`
Tests that converting a pandas-on-Spark series to pandas and back yields the same result.

#### `test_ge`
Tests greater-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_gt`
Tests greater-than comparisons between complex data types (arrays and structs).

#### `test_invert`
Tests that the bitwise NOT (invert) operator raises TypeError on this data type.

#### `test_isnull`
Tests notnull and isnull methods on a DataFrame.

#### `test_le`
Tests less-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_lt`
Tests less-than comparisons between complex data types (arrays and structs).

#### `test_mod`
Tests modulo operations between two series, sorting the index before comparison.

#### `test_mul`
Tests that multiplication operations on complex data types raise TypeError.

#### `test_ne`
Tests not-equal comparisons between complex data types (arrays and structs).

#### `test_neg`
Tests that the negation operator raises TypeError on this data type.

#### `test_or`
Tests bitwise OR operations between boolean series and with scalar values.

#### `test_pow`
Tests that exponentiation operations on complex data types raise TypeError.

#### `test_radd`
Tests that reflected addition raises TypeError.

#### `test_rand`
Tests that reflected bitwise AND raises TypeError.

#### `test_rfloordiv`
Tests reflected floor division with a scalar value.

#### `test_rmod`
Tests reflected modulo operations between two series.

#### `test_rmul`
Tests that reflected multiplication raises TypeError.

#### `test_ror`
Tests that reflected bitwise OR raises TypeError.

#### `test_rpow`
Tests that reflected exponentiation raises TypeError.

#### `test_rsub`
Tests that reflected subtraction raises TypeError.

#### `test_rtruediv`
Tests that reflected true division raises TypeError.

#### `test_sub`
Tests that subtraction operations on complex data types raise TypeError.

#### `test_truediv`
Tests that true division operations on complex data types raise TypeError.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/data_type_ops/test_parity_null_ops.py`

### Class: `NullOpsParityTests`

#### `test_abs`
Tests that taking the absolute value of a pandas-on-Spark index matches pandas, and that it raises an error for MultiIndex.

#### `test_add`
Tests addition operations for date operations, checking behavior for numeric array + numeric array, non-numeric + non-numeric, and numeric + non-numeric.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_astype`
Tests casting a string series to category and related operations.

#### `test_eq`
Tests equality comparisons between various dense and sparse vectors and matrices.

#### `test_floordiv`
Tests that floor division operations on complex data types raise TypeError.

#### `test_from_to_pandas`
Tests that converting a pandas-on-Spark series to pandas and back yields the same result.

#### `test_ge`
Tests greater-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_gt`
Tests greater-than comparisons between complex data types (arrays and structs).

#### `test_invert`
Tests that the bitwise NOT (invert) operator raises TypeError on this data type.

#### `test_isnull`
Tests notnull and isnull methods on a DataFrame.

#### `test_le`
Tests less-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_lt`
Tests less-than comparisons between complex data types (arrays and structs).

#### `test_mod`
Tests modulo operations between two series, sorting the index before comparison.

#### `test_mul`
Tests that multiplication operations on complex data types raise TypeError.

#### `test_ne`
Tests not-equal comparisons between complex data types (arrays and structs).

#### `test_neg`
Tests that the negation operator raises TypeError on this data type.

#### `test_pow`
Tests that exponentiation operations on complex data types raise TypeError.

#### `test_radd`
Tests that reflected addition raises TypeError.

#### `test_rfloordiv`
Tests reflected floor division with a scalar value.

#### `test_rmod`
Tests reflected modulo operations between two series.

#### `test_rmul`
Tests that reflected multiplication raises TypeError.

#### `test_rpow`
Tests that reflected exponentiation raises TypeError.

#### `test_rsub`
Tests that reflected subtraction raises TypeError.

#### `test_rtruediv`
Tests that reflected true division raises TypeError.

#### `test_sub`
Tests that subtraction operations on complex data types raise TypeError.

#### `test_truediv`
Tests that true division operations on complex data types raise TypeError.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/data_type_ops/test_parity_num_arithmetic.py`

### Class: `ArithmeticParityTests`

#### `test_add`
Tests addition operations for date operations, checking behavior for numeric array + numeric array, non-numeric + non-numeric, and numeric + non-numeric.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_sub`
Tests that subtraction operations on complex data types raise TypeError.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/data_type_ops/test_parity_num_mod.py`

### Class: `NumModParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_mod`
Tests modulo operations between two series, sorting the index before comparison.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/data_type_ops/test_parity_num_mul_div.py`

### Class: `NumMulDivParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_floordiv`
Tests that floor division operations on complex data types raise TypeError.

#### `test_mod`
Tests modulo operations between two series, sorting the index before comparison.

#### `test_mul`
Tests that multiplication operations on complex data types raise TypeError.

#### `test_truediv`
Tests that true division operations on complex data types raise TypeError.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/data_type_ops/test_parity_num_ops.py`

### Class: `FractionalExtensionOpsParityTests`

#### `test_abs`
Tests that taking the absolute value of a pandas-on-Spark index matches pandas, and that it raises an error for MultiIndex.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_astype`
Tests casting a string series to category and related operations.

#### `test_eq`
Tests equality comparisons between various dense and sparse vectors and matrices.

#### `test_from_to_pandas`
Tests that converting a pandas-on-Spark series to pandas and back yields the same result.

#### `test_ge`
Tests greater-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_gt`
Tests greater-than comparisons between complex data types (arrays and structs).

#### `test_invert`
Tests that the bitwise NOT (invert) operator raises TypeError on this data type.

#### `test_isnull`
Tests notnull and isnull methods on a DataFrame.

#### `test_le`
Tests less-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_lt`
Tests less-than comparisons between complex data types (arrays and structs).

#### `test_ne`
Tests not-equal comparisons between complex data types (arrays and structs).

#### `test_neg`
Tests that the negation operator raises TypeError on this data type.

### Class: `IntegralExtensionOpsParityTests`

#### `test_abs`
Tests that taking the absolute value of a pandas-on-Spark index matches pandas, and that it raises an error for MultiIndex.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_astype`
Tests casting a string series to category and related operations.

#### `test_eq`
Tests equality comparisons between various dense and sparse vectors and matrices.

#### `test_from_to_pandas`
Tests that converting a pandas-on-Spark series to pandas and back yields the same result.

#### `test_ge`
Tests greater-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_gt`
Tests greater-than comparisons between complex data types (arrays and structs).

#### `test_invert`
Tests that the bitwise NOT (invert) operator raises TypeError on this data type.

#### `test_isnull`
Tests notnull and isnull methods on a DataFrame.

#### `test_le`
Tests less-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_lt`
Tests less-than comparisons between complex data types (arrays and structs).

#### `test_ne`
Tests not-equal comparisons between complex data types (arrays and structs).

#### `test_neg`
Tests that the negation operator raises TypeError on this data type.

#### `test_rxor`
Tests reflected bitwise XOR operations with boolean values.

#### `test_xor`
Tests bitwise XOR operations between boolean series and with scalar values, including error cases for invalid types.

### Class: `NumOpsParityTests`

#### `test_abs`
Tests that taking the absolute value of a pandas-on-Spark index matches pandas, and that it raises an error for MultiIndex.

#### `test_and`
Tests bitwise AND operations between boolean series and with scalar values.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_comparison_dtype_compatibility`
Tests dtype compatibility in comparisons between int, bool, float, and str types.

#### `test_eq`
Tests equality comparisons between various dense and sparse vectors and matrices.

#### `test_from_to_pandas`
Tests that converting a pandas-on-Spark series to pandas and back yields the same result.

#### `test_ge`
Tests greater-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_gt`
Tests greater-than comparisons between complex data types (arrays and structs).

#### `test_invert`
Tests that the bitwise NOT (invert) operator raises TypeError on this data type.

#### `test_isnull`
Tests notnull and isnull methods on a DataFrame.

#### `test_le`
Tests less-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_lt`
Tests less-than comparisons between complex data types (arrays and structs).

#### `test_ne`
Tests not-equal comparisons between complex data types (arrays and structs).

#### `test_neg`
Tests that the negation operator raises TypeError on this data type.

#### `test_or`
Tests bitwise OR operations between boolean series and with scalar values.

#### `test_rand`
Tests that reflected bitwise AND raises TypeError.

#### `test_ror`
Tests that reflected bitwise OR raises TypeError.

#### `test_rxor`
Tests reflected bitwise XOR operations with boolean values.

#### `test_xor`
Tests bitwise XOR operations between boolean series and with scalar values, including error cases for invalid types.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/data_type_ops/test_parity_num_pow.py`

### Class: `NumPowParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_pow`
Tests that exponentiation operations on complex data types raise TypeError.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/data_type_ops/test_parity_num_reverse.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `ReverseParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_radd`
Tests that reflected addition raises TypeError.

#### `test_rfloordiv`
Tests reflected floor division with a scalar value.

#### `test_rmod`
Tests reflected modulo operations between two series.

#### `test_rmul`
Tests that reflected multiplication raises TypeError.

#### `test_rpow`
Tests that reflected exponentiation raises TypeError.

#### `test_rsub`
Tests that reflected subtraction raises TypeError.

#### `test_rtruediv`
Tests that reflected true division raises TypeError.

## File: `python/pyspark/pandas/tests/connect/data_type_ops/test_parity_string_ops.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `StringExtensionOpsParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_astype`
Tests casting a string series to category and related operations.

#### `test_eq`
Tests equality comparisons between various dense and sparse vectors and matrices.

#### `test_from_to_pandas`
Tests that converting a pandas-on-Spark series to pandas and back yields the same result.

#### `test_ge`
Tests greater-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_gt`
Tests greater-than comparisons between complex data types (arrays and structs).

#### `test_isnull`
Tests notnull and isnull methods on a DataFrame.

#### `test_le`
Tests less-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_lt`
Tests less-than comparisons between complex data types (arrays and structs).

#### `test_mul`
Tests that multiplication operations on complex data types raise TypeError.

#### `test_ne`
Tests not-equal comparisons between complex data types (arrays and structs).

#### `test_radd`
Tests that reflected addition raises TypeError.

### Class: `StringOpsParityTests`

#### `test_abs`
Tests that taking the absolute value of a pandas-on-Spark index matches pandas, and that it raises an error for MultiIndex.

#### `test_add`
Tests addition operations for date operations, checking behavior for numeric array + numeric array, non-numeric + non-numeric, and numeric + non-numeric.

#### `test_and`
Tests bitwise AND operations between boolean series and with scalar values.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_astype`
Tests casting a string series to category and related operations.

#### `test_eq`
Tests equality comparisons between various dense and sparse vectors and matrices.

#### `test_floordiv`
Tests that floor division operations on complex data types raise TypeError.

#### `test_from_to_pandas`
Tests that converting a pandas-on-Spark series to pandas and back yields the same result.

#### `test_from_to_pandas_with_missing_values`
Tests converting a pandas-on-Spark series with missing values to pandas.

#### `test_ge`
Tests greater-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_gt`
Tests greater-than comparisons between complex data types (arrays and structs).

#### `test_invert`
Tests that the bitwise NOT (invert) operator raises TypeError on this data type.

#### `test_isnull`
Tests notnull and isnull methods on a DataFrame.

#### `test_le`
Tests less-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_lt`
Tests less-than comparisons between complex data types (arrays and structs).

#### `test_mod`
Tests modulo operations between two series, sorting the index before comparison.

#### `test_mul`
Tests that multiplication operations on complex data types raise TypeError.

#### `test_ne`
Tests not-equal comparisons between complex data types (arrays and structs).

#### `test_neg`
Tests that the negation operator raises TypeError on this data type.

#### `test_or`
Tests bitwise OR operations between boolean series and with scalar values.

#### `test_pow`
Tests that exponentiation operations on complex data types raise TypeError.

#### `test_radd`
Tests that reflected addition raises TypeError.

#### `test_rand`
Tests that reflected bitwise AND raises TypeError.

#### `test_rfloordiv`
Tests reflected floor division with a scalar value.

#### `test_rmod`
Tests reflected modulo operations between two series.

#### `test_rmul`
Tests that reflected multiplication raises TypeError.

#### `test_ror`
Tests that reflected bitwise OR raises TypeError.

#### `test_rpow`
Tests that reflected exponentiation raises TypeError.

#### `test_rsub`
Tests that reflected subtraction raises TypeError.

#### `test_rtruediv`
Tests that reflected true division raises TypeError.

#### `test_sub`
Tests that subtraction operations on complex data types raise TypeError.

#### `test_truediv`
Tests that true division operations on complex data types raise TypeError.

## File: `python/pyspark/pandas/tests/connect/data_type_ops/test_parity_timedelta_ops.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `TimedeltaOpsParityTests`

#### `test_abs`
Tests that taking the absolute value of a pandas-on-Spark index matches pandas, and that it raises an error for MultiIndex.

#### `test_add`
Tests addition operations for date operations, checking behavior for numeric array + numeric array, non-numeric + non-numeric, and numeric + non-numeric.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_astype`
Tests casting a string series to category and related operations.

#### `test_eq`
Tests equality comparisons between various dense and sparse vectors and matrices.

#### `test_floordiv`
Tests that floor division operations on complex data types raise TypeError.

#### `test_from_to_pandas`
Tests that converting a pandas-on-Spark series to pandas and back yields the same result.

#### `test_ge`
Tests greater-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_gt`
Tests greater-than comparisons between complex data types (arrays and structs).

#### `test_invert`
Tests that the bitwise NOT (invert) operator raises TypeError on this data type.

#### `test_isnull`
Tests notnull and isnull methods on a DataFrame.

#### `test_le`
Tests less-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_lt`
Tests less-than comparisons between complex data types (arrays and structs).

#### `test_mod`
Tests modulo operations between two series, sorting the index before comparison.

#### `test_mul`
Tests that multiplication operations on complex data types raise TypeError.

#### `test_ne`
Tests not-equal comparisons between complex data types (arrays and structs).

#### `test_neg`
Tests that the negation operator raises TypeError on this data type.

#### `test_pow`
Tests that exponentiation operations on complex data types raise TypeError.

#### `test_radd`
Tests that reflected addition raises TypeError.

#### `test_rfloordiv`
Tests reflected floor division with a scalar value.

#### `test_rmod`
Tests reflected modulo operations between two series.

#### `test_rmul`
Tests that reflected multiplication raises TypeError.

#### `test_rpow`
Tests that reflected exponentiation raises TypeError.

#### `test_rsub`
Tests that reflected subtraction raises TypeError.

#### `test_rtruediv`
Tests that reflected true division raises TypeError.

#### `test_sub`
Tests that subtraction operations on complex data types raise TypeError.

#### `test_truediv`
Tests that true division operations on complex data types raise TypeError.

## File: `python/pyspark/pandas/tests/connect/data_type_ops/test_parity_udt_ops.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `UDTOpsParityTests`

#### `test_abs`
Tests that taking the absolute value of a pandas-on-Spark index matches pandas, and that it raises an error for MultiIndex.

#### `test_add`
Tests addition operations for date operations, checking behavior for numeric array + numeric array, non-numeric + non-numeric, and numeric + non-numeric.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_astype`
Tests casting a string series to category and related operations.

#### `test_eq`
Tests equality comparisons between various dense and sparse vectors and matrices.

#### `test_floordiv`
Tests that floor division operations on complex data types raise TypeError.

#### `test_from_to_pandas`
Tests that converting a pandas-on-Spark series to pandas and back yields the same result.

#### `test_ge`
Tests greater-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_gt`
Tests greater-than comparisons between complex data types (arrays and structs).

#### `test_invert`
Tests that the bitwise NOT (invert) operator raises TypeError on this data type.

#### `test_isnull`
Tests notnull and isnull methods on a DataFrame.

#### `test_le`
Tests less-than-or-equal comparisons between complex data types (arrays and structs).

#### `test_lt`
Tests less-than comparisons between complex data types (arrays and structs).

#### `test_mod`
Tests modulo operations between two series, sorting the index before comparison.

#### `test_mul`
Tests that multiplication operations on complex data types raise TypeError.

#### `test_ne`
Tests not-equal comparisons between complex data types (arrays and structs).

#### `test_neg`
Tests that the negation operator raises TypeError on this data type.

#### `test_pow`
Tests that exponentiation operations on complex data types raise TypeError.

#### `test_radd`
Tests that reflected addition raises TypeError.

#### `test_rfloordiv`
Tests reflected floor division with a scalar value.

#### `test_rmod`
Tests reflected modulo operations between two series.

#### `test_rmul`
Tests that reflected multiplication raises TypeError.

#### `test_rpow`
Tests that reflected exponentiation raises TypeError.

#### `test_rsub`
Tests that reflected subtraction raises TypeError.

#### `test_rtruediv`
Tests that reflected true division raises TypeError.

#### `test_sub`
Tests that subtraction operations on complex data types raise TypeError.

#### `test_truediv`
Tests that true division operations on complex data types raise TypeError.

#### `test_with_all_null`
Tests that a series with all null values can be converted to pandas and back.

#### `test_with_first_null`
Tests that a series or DataFrame with the first value being null (and others being SparseVectors) can be converted to pandas and back.

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_align.py`

### Class: `DiffFramesParityAlignTests`

#### `test_align`
Tests the align method for DataFrames and Series with various join types and axes.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_arithmetic.py`

### Class: `ArithmeticParityTests`

#### `test_arithmetic`
Tests basic arithmetic operations (+, -, *, /) between DataFrames and Series.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_multi_index_arithmetic`
Tests arithmetic operations between Series and DataFrames with MultiIndex.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_arithmetic_chain.py`

### Class: `ArithmeticChainParityTests`

#### `test_arithmetic_chain`
Tests chained arithmetic operations on DataFrames and Series.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_arithmetic_chain_ext.py`

### Class: `ArithmeticChainExtParityTests`

#### `test_arithmetic_chain_extension_dtypes`
Tests chained arithmetic operations on DataFrames and Series with nullable integer extension dtypes.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_arithmetic_chain_ext_float.py`

### Class: `ArithmeticChainExtFloatParityTests`

#### `test_arithmetic_chain_extension_float_dtypes`
Tests chained arithmetic operations on DataFrames and Series with nullable float extension dtypes.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_arithmetic_ext.py`

### Class: `ArithmeticExtParityTests`

#### `test_arithmetic`
Tests basic arithmetic operations (+, -, *, /) between DataFrames and Series.

#### `test_arithmetic_extension_dtypes`
Tests arithmetic operations on DataFrames and Series with nullable integer extension dtypes.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_multi_index_arithmetic`
Tests arithmetic operations between Series and DataFrames with MultiIndex.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_arithmetic_ext_float.py`

### Class: `ArithmeticExtFloatParityTests`

#### `test_arithmetic`
Tests basic arithmetic operations (+, -, *, /) between DataFrames and Series.

#### `test_arithmetic_extension_float_dtypes`
Tests arithmetic operations on DataFrames and Series with nullable float extension dtypes.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_multi_index_arithmetic`
Tests arithmetic operations between Series and DataFrames with MultiIndex.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_assign_frame.py`

### Class: `AssignFrameParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_assignment_frame`
Tests assigning a DataFrame to a subset of columns of another DataFrame.

#### `test_assignment_frame_chain`
Tests chained assignments of DataFrames to columns of another DataFrame.

#### `test_multi_index_assignment_frame`
Tests assigning a DataFrame to columns of another DataFrame with MultiIndex.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_assign_series.py`

### Class: `AssignSeriesParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_assignment_series`
Tests assigning a Series to a column of a DataFrame.

#### `test_assignment_series_chain`
Tests chained assignments of Series to columns of a DataFrame.

#### `test_multi_index_assignment_series`
Tests assigning a Series to a column of a DataFrame with MultiIndex.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_basic.py`

### Class: `BasicParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_different_columns`
Tests addition of DataFrames with different columns, including MultiIndex columns.

#### `test_getitem_boolean_series`
Tests boolean indexing on a DataFrame using a Series from a different DataFrame.

#### `test_insert`
Tests inserting a value into a CategoricalIndex.

#### `test_loc_getitem_boolean_series`
Tests .loc with a boolean Series from another DataFrame.

#### `test_mod`
Tests modulo operations between two series, sorting the index before comparison.

#### `test_no_matched_index`
Tests that adding DataFrames with mismatched index names raises a ValueError.

#### `test_ranges`
Tests addition of ranges (DataFrames created by range).

#### `test_rmod`
Tests reflected modulo operations between two series.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_basic_slow.py`

### Class: `DiffFramesParityBasicSlowTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_diff`
Tests the diff method on a grouped DataFrame.

#### `test_mask`
Tests that mask with an invalid condition type raises TypeError.

#### `test_pow_and_rpow`
Tests power and reflected power operations between Series.

#### `test_rank`
Tests the rank method on a grouped DataFrame.

#### `test_shift`
Tests the shift method on a grouped DataFrame.

#### `test_to_series_comparison`
Tests comparison of series generated by to_series().

#### `test_update`
Tests the update method for DataFrames, including multi-index columns and overwrite option.

#### `test_where`
Tests that calling where on a Spark DataFrame with an invalid condition raises an error.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_bitwise.py`

### Class: `BitwiseParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_bitwise`
Tests bitwise operations (& and |) between boolean Series.

#### `test_bitwise_extension_dtype`
Tests bitwise operations between boolean Series using extension data types.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_combine_first.py`

### Class: `CombineFirstParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_combine_first`
Tests the combine_first method on DataFrames.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_compare_series.py`

### Class: `CompareSeriesParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_compare`
Tests the compare method for Series, including various options and MultiIndex cases.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_concat_inner.py`

### Class: `ConcatInnerParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_concat_column_axis_inner`
Tests concatenation of DataFrames along columns with an inner join.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_concat_outer.py`

### Class: `ConcatOuterParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_concat_column_axis_outer`
Tests concatenation of DataFrames along columns with an outer join.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_corrwith.py`

### Class: `DiffFramesParityCorrWithTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_corrwith`
Tests the corrwith method between DataFrames and Series.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_cov.py`

### Class: `DiffFramesParityCovTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_cov`
Tests the cov (covariance) method for Spark DataFrames.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_dot_frame.py`

### Class: `DiffFramesParityDotFrameTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_frame_dot`
Tests matrix multiplication (dot product) between a DataFrame and a Series or another DataFrame.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_dot_series.py`

### Class: `DiffFramesParityDotSeriesTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_series_dot`
Tests dot product of two series, or a series and a dataframe.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_error.py`

### Class: `DiffFramesErrorParityTests`

#### `test_align`
Tests the align method for DataFrames and Series with various join types and axes.

#### `test_arithmetic`
Tests basic arithmetic operations (+, -, *, /) between DataFrames and Series.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_assignment`
Tests that assigning a Series from a DataFrame to another DataFrame raises an error when they cannot be combined.

#### `test_combine_first`
Tests the combine_first method on DataFrames.

#### `test_equals`
Tests internal _equals method for Vectors.

#### `test_frame_iloc_setitem`
Tests setting items in a DataFrame using .iloc.

#### `test_frame_loc_setitem`
Tests setting items in a DataFrame using .loc.

#### `test_mask`
Tests that mask with an invalid condition type raises TypeError.

#### `test_pow_and_rpow`
Tests power and reflected power operations between Series.

#### `test_series_eq`
Tests equality operations on Series with different index types.

#### `test_series_iloc_setitem`
Tests setting items in a Series using .iloc.

#### `test_series_loc_setitem`
Tests setting items in a Series using .loc.

#### `test_where`
Tests that calling where on a Spark DataFrame with an invalid condition raises an error.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_groupby.py`

### Class: `GroupByParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_duplicated_labels`
Tests groupby with duplicated labels in the grouping key.

#### `test_groupby_multiindex_columns`
Tests groupby on a DataFrame with MultiIndex columns.

#### `test_head`
Tests the head method on a DataFrame in the Python client.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_groupby_aggregate.py`

### Class: `GroupByAggregateParityTests`

#### `test_aggregate`
Tests the agg (aggregate) method on a grouped DataFrame.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_groupby_apply.py`

### Class: `GroupByApplyParityTests`

#### `test_apply`
Tests applying a function to a grouped DataFrame, verifying behavior across pandas versions regarding include_groups.

#### `test_apply_without_shortcut`
Tests apply with the compute shortcut disabled (shortcut limit set to 0).

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_groupby_cumulative.py`

### Class: `GroupByCumulativeParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_cumcount`
Tests cumulative count on a grouped DataFrame with various options like ascending/descending and MultiIndex.

#### `test_cummax`
Tests cumulative maximum on a grouped DataFrame.

#### `test_cummin`
Tests cumulative minimum on a grouped DataFrame.

#### `test_cumprod`
Tests cumulative product on a grouped DataFrame.

#### `test_cumsum`
Tests cumulative sum on a grouped DataFrame.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_groupby_diff.py`

### Class: `GroupByDiffParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_diff`
Tests the diff method on a grouped DataFrame.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_groupby_diff_len.py`

### Class: `GroupByDiffLenParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_groupby_different_lengths`
Tests groupby with DataFrames of different lengths, verifying alignment and results.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_groupby_expanding.py`

### Class: `GroupByExpandingParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_groupby_expanding_max`
Tests expanding maximum on a grouped DataFrame.

#### `test_groupby_expanding_mean`
Tests expanding mean on a grouped DataFrame.

#### `test_groupby_expanding_min`
Tests expanding minimum on a grouped DataFrame.

#### `test_groupby_expanding_sum`
Tests expanding sum on a grouped DataFrame.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_groupby_expanding_adv.py`

### Class: `GroupByExpandingAdvParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_groupby_expanding_std`
Tests expanding standard deviation on a grouped DataFrame.

#### `test_groupby_expanding_var`
Tests expanding variance on a grouped DataFrame.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_groupby_expanding_count.py`

### Class: `GroupByExpandingCountParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_groupby_expanding_count`
Tests expanding count on a grouped DataFrame.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_groupby_fillna.py`

### Class: `GroupByFillNAParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_fillna`
Tests fillna with various types (int, double, bool, string) and subsets on a DataFrame.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_groupby_filter.py`

### Class: `GroupByFilterParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_filter`
Tests filtering operations in Spark Connect, checking plan generation and unresolved functions.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_groupby_rolling.py`

### Class: `GroupByRollingParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_groupby_rolling_max`
Tests rolling maximum on a grouped DataFrame.

#### `test_groupby_rolling_mean`
Tests rolling mean on a grouped DataFrame.

#### `test_groupby_rolling_min`
Tests rolling minimum on a grouped DataFrame.

#### `test_groupby_rolling_sum`
Tests rolling sum on a grouped DataFrame.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_groupby_rolling_adv.py`

### Class: `GroupByRollingAdvParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_groupby_rolling_std`
Tests rolling standard deviation on a grouped DataFrame.

#### `test_groupby_rolling_var`
Tests rolling variance on a grouped DataFrame.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_groupby_rolling_count.py`

### Class: `GroupByRollingCountParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_groupby_rolling_count`
Tests rolling count on a grouped DataFrame, with some TODO notes regarding min_periods.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_groupby_shift.py`

### Class: `GroupByShiftParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_shift`
Tests the shift method on a grouped DataFrame.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_groupby_split_apply_combine.py`

### Class: `GroupBySACParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_split_apply_combine_on_series`
Tests split-apply-combine operations on a Series using variance.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_groupby_transform.py`

### Class: `GroupByTransformParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_transform`
Tests the transform method with built-in and lambda functions.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_index.py`

### Class: `DiffFramesParityIndexTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_index_ops`
Tests operations on Index objects, including addition and MultiIndex level access.

#### `test_multi_index_column_assignment_frame`
Tests multi-index column assignment on a DataFrame.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_series.py`

### Class: `DiffFramesParitySeriesTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_series_eq`
Tests equality operations between Series and other objects (Series, Index).

#### `test_series_ops`
Tests arithmetic operations between Series and Index objects, checking for broadcast errors when shapes mismatch.

#### `test_series_repeat`
Tests the repeat method on a Series.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_setitem_frame.py`

### Class: `DiffFramesParitySetItemFrameTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_frame_iloc_setitem`
Tests setting values in a DataFrame using .iloc.

#### `test_frame_loc_setitem`
Tests setting values in a DataFrame using .loc.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/diff_frames_ops/test_parity_setitem_series.py`

### Class: `DiffFramesParitySetItemSeriesTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_series_iloc_setitem`
Tests setting values in a Series using .iloc.

#### `test_series_loc_setitem`
Tests setting values in a Series using .loc.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/frame/test_parity_asfreq.py`

### Class: `AsFreqParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_disabled`
Tests that calling asfreq without fallback enabled raises PandasNotImplementedError.

#### `test_fallback`
Tests asfreq with compute fallback enabled, checking correct results against pandas.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/frame/test_parity_asof.py`

### Class: `AsOfParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_disabled`
Tests that asfreq (likely a typo in source or test name reused) is disabled.

#### `test_fallback`
Tests asfreq fallback behavior (consistent with line 40).

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/frame/test_parity_attrs.py`

### Class: `FrameParityAttrsTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_assign`
Tests assigning columns to a DataFrame, including multi-index cases and method chaining via assign().

#### `test_attributes`
Tests DataFrame attribute access (e.g., column names as attributes) and error cases.

#### `test_axes`
Tests the axes property of DataFrames, including MultiIndex columns.

#### `test_column_names`
Tests column names preservation and generation during operations.

#### `test_dataframe_column_level_name`
Tests DataFrame column level names.

#### `test_dataframe_multiindex_columns`
Tests column access on DataFrames with MultiIndex.

#### `test_dataframe_multiindex_names_level`
Tests name retrieval for MultiIndex columns in DataFrames.

#### `test_dtype`
Tests proper data types preservation across DataFrame operations.

#### `test_empty_timestamp`
Tests that filtering a DataFrame with a timestamp column for non-equal values returns an empty DataFrame with matching dtypes, consistent with Pandas.

#### `test_inplace`
Tests that in-place addition on a column of a DataFrame updates both the DataFrame and a reference to that column correctly.

#### `test_multi_index_dtypes`
Tests that dtypes property of a MultiIndex works correctly for multi-index columns with both single and multiple labels.

#### `test_multi_index_dtypes_not_unique_name`
Tests that dtypes property of a MultiIndex works correctly when level names are not unique.

#### `test_rename_columns`
Tests renaming columns of a DataFrame, including setting column names directly and using a pd.Index with a name, and verifies behavior with multi-index columns.

#### `test_repr_cache_invalidation`
Tests that modifying a DataFrame in-place correctly invalidates its __repr__ cache.

#### `test_repr_html_cache_invalidation`
Tests that modifying a DataFrame in-place correctly invalidates its HTML representation cache.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/frame/test_parity_axis.py`

### Class: `FrameParityAxisTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_axis_on_dataframe`
Tests row-wise (axis=1) reduction operations (e.g., count, sum, mean, std) on a DataFrame with a large number of rows to ensure parity with Pandas, including support for numeric_only.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/frame/test_parity_constructor.py`

### Class: `FrameParityConstructorTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_astype_extension_dtypes`
Tests that casting DataFrame columns to nullable integer extension dtypes (e.g., 'Int8', 'Int64') works correctly.

#### `test_astype_extension_float_dtypes`
Tests that casting DataFrame columns to nullable float extension dtypes (e.g., 'Float32', 'Float64') works correctly.

#### `test_astype_extension_object_dtypes`
Tests that casting DataFrame columns to 'string' and 'boolean' extension dtypes works correctly.

#### `test_creation_index`
Extensively tests DataFrame creation with various index types (Pandas Index and pandas-on-Spark Index) and data sources, including testing error conditions when combining distributed datasets with indices under different configuration options.

#### `test_creation_index_same_anchor`
Tests creating a new DataFrame from an existing DataFrame while reusing the existing DataFrame's index, covering various index types like string, datetime, timedelta, and categorical indices.

#### `test_dataframe`
Tests general DataFrame operations like basic arithmetic, column access, boolean indexing, reduction operations, and DataFrame creation from a Series, including verifying that adding new columns via attribute assignment is restricted.

#### `test_extension_dtypes`
Tests that creating a DataFrame with nullable integer extension dtypes preserves those types and operations on them work as expected.

#### `test_extension_float_dtypes`
Tests that creating a DataFrame with nullable float extension dtypes preserves those types and arithmetic operations on them work as expected.

#### `test_extension_object_dtypes`
Tests that creating a DataFrame with 'string' and 'boolean' extension dtypes preserves those types.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/frame/test_parity_conversion.py`

### Class: `FrameParityConversionTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_astype`
Tests casting a Series to categorical types, including specifying categories order and casting back to string.

#### `test_isnull`
Tests that isnull and notnull functions correctly identify missing and non-missing values in a DataFrame.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/frame/test_parity_interpolate.py`

### Class: `FrameInterpolateParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_interpolate`
Tests the interpolate method on DataFrames with various data distributions including NaN values.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/frame/test_parity_interpolate_error.py`

### Class: `FrameInterpolateErrorParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_interpolate_error`
Tests that the interpolate method correctly raises appropriate errors for unsupported methods (e.g., non-linear), invalid limit arguments, or when attempted on columns with only object dtypes.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/frame/test_parity_reindexing.py`

### Class: `FrameParityReindexingTests`

#### `test_add_prefix`
Tests that add_prefix correctly prepends a string to column labels, supporting multi-index columns.

#### `test_add_suffix`
Tests that add_suffix correctly appends a string to column labels, supporting multi-index columns.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_at_time`
Tests the at_time method on a DataFrame with a DatetimeIndex to select values at a particular time of day, verifying behavior with various index and column naming combinations and error conditions.

#### `test_between_time`
Tests the between_time method to select values between specific times of day, including support for different inclusive bounds, and verifies proper handling of error cases such as non-DatetimeIndex instances.

#### `test_drop`
Tests the drop method on a DataFrame in the context of Spark Connect, verifying that the logical plan correctly identifies columns to be dropped by name or reference.

#### `test_drop_duplicates`
Tests drop_duplicates on a Spark DataFrame, ensuring duplicates are removed based on a subset of columns, and verifies that appropriate errors are raised for invalid subset types.

#### `test_drop_with_errors`
Tests the drop method with different error handling policies (errors='ignore' or 'raise') when attempting to drop non-existent columns or rows.

#### `test_droplevel`
Tests the droplevel method to remove specified levels from MultiIndex columns or indices, including handling non-string level names and verifying error conditions.

#### `test_duplicated`
Tests identifying duplicated rows in a DataFrame, supporting arguments like keep and subset, and verifies behavior with multi-indices and multi-index columns.

#### `test_filter`
Tests that a filter operation on a DataFrame in Spark Connect generates a correct plan containing the expected filter condition.

#### `test_first`
Tests the first action on a DataFrame in Spark Connect, returning the first row or None for an empty result.

#### `test_isin`
Tests the isin method to filter based on whether column values are present in a given list of literals or values from another column, comparing against standard Spark.

#### `test_last`
Tests that the last method in a groupby operation correctly computes the last non-null element for each group, supporting numeric_only and min_count parameters.

#### `test_sample`
Tests the random sampling of rows from a DataFrame with or without replacement and a given seed, and verifies that parameters are correctly reflected in the plan.

#### `test_swapaxes`
Tests swapaxes method on a DataFrame, ensuring it correctly interchanges axes, or raises an AttributeError on newer pandas versions where it is likely removed.

#### `test_swaplevel`
Tests the swaplevel method to swap two levels of a MultiIndex for either axis (index or columns), covering edge cases and verifying expected error handling.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/frame/test_parity_reshaping.py`

### Class: `FrameParityReshapingTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_assign_list`
Tests assigning a list as a new column in a DataFrame, verifying that length mismatch errors are raised when applicable.

#### `test_explode`
Tests standard Spark SQL functions explode, posexplode_outer, and explode_outer for arrays and maps to verify they properly flatten complex column types.

#### `test_nlargest`
Tests that nlargest on a grouped Series correctly identifies the largest elements per group, ensuring edge cases like multi-index limitations are enforced via raised errors.

#### `test_nsmallest`
Tests that nsmallest on a grouped Series correctly identifies the smallest elements per group, ensuring edge cases like multi-index limitations are enforced via raised errors.

#### `test_sort_index`
Tests the sort_index method on DataFrames with both single and multi-level indices, exploring options like descending sort, NA positioning, sorting inplace, and ignoring index.

#### `test_sort_values`
Tests the sort_values method on DataFrames and indices, covering standard indices, named indices, and multi-indices.

#### `test_squeeze`
Tests the squeeze method on DataFrames with various dimensions to ensure redundant dimensions of length 1 are properly removed along specified axes.

#### `test_stack`
Tests a specific Spark Connect Table Valued Function stack against a direct SQL query to ensure consistency.

#### `test_transpose`
Tests the transpose method on a DataFrame, ensuring it properly pivots rows and columns, enforces row limits, and checks for least common types across non-index columns.

#### `test_unstack`
Tests the unstack method on a multi-indexed Series to transform it back into a DataFrame.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/frame/test_parity_spark.py`

### Class: `FrameParitySparkTests`

#### `test_all_null_dataframe`
Tests that a DataFrame containing predominantly None (null) values in various type columns is handled consistently with and without Arrow optimization enabled.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_cache`
Tests the behavior of ModelCache to ensure it properly retains a fixed maximum number of model functions and updates their order on retrieval (LRU policy).

#### `test_dot_in_column_name`
Tests that columns with a dot ('.') in their name are correctly handled by pandas-on-Spark when selected via expression.

#### `test_empty_dataframe`
Verifies that creating a pandas-on-Spark DataFrame from an empty pandas DataFrame works correctly, both with and without Arrow enabled.

#### `test_explain_hint`
Verifies that the `spark.explain()` output contains 'Broadcast' when a broadcast hint is applied to a merged DataFrame.

#### `test_missing`
Verifies that calling unsupported pandas-like general functions in pandas-on-Spark raises `PandasNotImplementedError`.

#### `test_nullable_object`
Verifies that creating a pandas-on-Spark DataFrame with various nullable object types from a pandas DataFrame works correctly, both with and without Arrow enabled.

#### `test_persist`
Verifies that the `spark.persist()` API works correctly with different storage levels and raises a `TypeError` for invalid storage level types.

#### `test_print_schema`
Verifies that the `treeString()` representation of a DataFrame schema matches the expected string.

#### `test_spark_schema`
Verifies that the `spark.schema()` API returns the correct `StructType` schema for the DataFrame, including options to specify the index name.

#### `test_to_pandas_with_nullable_string_column`
Verifies that converting a pandas-on-Spark DataFrame with a nullable string column back to a pandas DataFrame works correctly and maintains data types, both with and without Arrow enabled.

#### `test_udt`
Calls the superclass test for User Defined Types (UDT).

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/frame/test_parity_take.py`

### Class: `FrameTakeParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_take`
Verifies the `take` API in the Python client by asserting the number of rows returned for non-empty and empty DataFrames.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/frame/test_parity_take_adv.py`

### Class: `FrameTakeAdvParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_take_adv`
Extensively tests the `take` API with MultiIndex columns, both along axis 0 and 1, with positive and negative indices, and verifies that invalid input types raise `TypeError`.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/frame/test_parity_time_series.py`

### Class: `FrameParityTimeSeriesTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_first_valid_index`
Verifies that `first_valid_index` returns the correct index for DataFrames with missing values, MultiIndex columns, empty DataFrames, and datetime indices.

#### `test_last_valid_index`
Verifies that `last_valid_index` returns the correct index for DataFrames with missing values, MultiIndex columns, and empty DataFrames.

#### `test_shift`
Verifies the `shift` operation on grouped DataFrames with various parameters, grouped by different column types, and with MultiIndex columns.

#### `test_to_datetime`
Verifies that `to_datetime` works correctly for various input formats like DataFrames, dictionaries, Unix timestamps, and lists, with different units and origins.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/frame/test_parity_truncate.py`

### Class: `FrameParityTruncateTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_truncate`
Verifies the `truncate` API for DataFrames with sorted indices, both along axis 0 and 1, with and without copying, and with MultiIndex columns. Also verifies exceptions for unsorted indices or invalid bounds.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_aggregate.py`

### Class: `GroupbyParityAggregateTests`

#### `test_aggregate`
Extensively tests the `agg` (aggregate) API for grouped DataFrames with various combinations of functions, `as_index` parameter, MultiIndex columns, and non-string column names.

#### `test_aggregate_func_str_list`
Verifies `agg` with simple string functions or lists of strings on both single and multi-index column DataFrames.

#### `test_aggregate_relabel`
Verifies named aggregation (relabeling columns) in `groupby.agg`, including use of `NamedAgg`.

#### `test_aggregate_relabel_multiindex`
Verifies named aggregation in `groupby.agg` when operating on DataFrames with MultiIndex columns.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_apply_func.py`

### Class: `GroupbyParityApplyFuncTests`

#### `test_apply`
Verifies the `apply` API on grouped DataFrames, taking into account different behaviors based on the pandas version and `include_groups` parameter.

#### `test_apply_explicitly_infer`
Verifies that `apply` works when Arrow is disabled by explicitly inferring the schema.

#### `test_apply_infer_schema_without_shortcut`
Verifies schema inference in `groupby.apply` when computation shortcut limit is set to 0.

#### `test_apply_key_handling`
Verifies key handling in `groupby.apply` by comparing with pandas behavior when applying a sum function.

#### `test_apply_key_handling_without_shortcut`
Calls `test_apply_key_handling` with computation shortcut limit set to 0.

#### `test_apply_negative`
Verifies that using a return type hint of `ps.Series` in `apply` on a DataFrame groupby raises a `TypeError`.

#### `test_apply_return_series`
Verifies `apply` when the applied function returns a Series, handling pandas version differences and the `include_groups` parameter.

#### `test_apply_return_series_with_multi_index_columns`
Similar to `test_apply_return_series` but with MultiIndex columns.

#### `test_apply_return_series_with_multi_index_columns_without_shortcut`
Calls `test_apply_return_series_with_multi_index_columns` with a non-zero computation shortcut limit.

#### `test_apply_return_series_without_shortcut`
Calls `test_apply_return_series` with a non-zero computation shortcut limit.

#### `test_apply_with_multi_index_columns`
Verifies `apply` on DataFrames with MultiIndex columns, handling pandas version differences.

#### `test_apply_with_multi_index_columns_without_shortcut`
Calls `test_apply_with_multi_index_columns` with computation shortcut limit set to 0.

#### `test_apply_with_new_dataframe`
Verifies that `apply` returning a new DataFrame works correctly, including cases with large DataFrames (1000+ records).

#### `test_apply_with_new_dataframe_without_shortcut`
Calls `test_apply_with_new_dataframe` with computation shortcut limit set to 0.

#### `test_apply_with_side_effect`
Calls the superclass test for testing apply with side effects.

#### `test_apply_with_side_effect_without_shortcut`
Calls `test_apply_with_side_effect` with computation shortcut limit set to 0.

#### `test_apply_with_type_hint`
Verifies `apply` with type hints, handling pandas version differences.

#### `test_apply_without_shortcut`
Calls `test_apply` with computation shortcut limit set to 0.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_filter`
Verifies that filtering a connect DataFrame generates a protobuf plan with the expected unresolved function and arguments.

#### `test_transform`
Verifies the `transform` API on Series from a connect DataFrame with both built-in and lambda functions.

#### `test_transform_without_shortcut`
Calls `test_transform` with computation shortcut limit set to 0.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_corr.py`

### Class: `CorrParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_corr`
Verifies calculating Pearson correlation between two columns using `stat.corr`.

#### `test_method`
Verifies `groupby.corr` with different correlation methods (pearson, spearman, kendall).

#### `test_min_periods`
Verifies `groupby.corr` with different `min_periods` values.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_cumulative.py`

### Class: `GroupbyParityCumulativeTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_cumcount`
Verifies `cumcount` on grouped DataFrames with both ascending and descending order, and with MultiIndex columns.

#### `test_cummax`
Verifies `cummax` on grouped DataFrames, including MultiIndex columns and verifying that non-numeric columns raise DataError.

#### `test_cummin`
Verifies `cummin` on grouped DataFrames, including MultiIndex columns and verifying that non-numeric columns raise DataError.

#### `test_cumprod`
Verifies `cumprod` on grouped DataFrames, including MultiIndex columns and verifying that non-numeric columns raise DataError.

#### `test_cumsum`
Verifies `cumsum` on grouped DataFrames, including MultiIndex columns and verifying that non-numeric columns raise DataError.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_describe.py`

### Class: `GroupbyParityDescribeTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_describe`
Verifies that describing filtered DataFrames produces the expected column lists in the generated protobuf plan.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_groupby.py`

### Class: `GroupByParityTests`

#### `test_aggregate_relabel_index_false`
Tests groupby with as_index=False and named aggregation (relabelling) in pandas-on-Spark.

#### `test_all_any`
Tests all() and any() methods on GroupBy objects, covering multi-index columns and skipna.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_diff`
Tests diff() method on grouped data, supporting single and multiple keys, and multi-index columns.

#### `test_groupby_simple`
Comprehensive test for simple groupby operations, including sorting and error handling.

#### `test_is_multi_agg_with_relabel`
Tests the internal helper function is_multi_agg_with_relabel for identifying multi-aggregation with renaming.

#### `test_nunique`
Tests nunique() on grouped data, including with multi-index columns and different dropna settings.

#### `test_shift`
Tests shift() on grouped data with various options like periods and fill values.

#### `test_unique`
Tests unique() on a SeriesGroupBy object for both numeric and string data.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_grouping.py`

### Class: `GroupingParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_get_group`
Tests get_group() on a GroupBy object, verifying group extraction and error handling.

#### `test_getitem`
Verifies that __getitem__ on a DataFrameGroupBy with a single column returns a SeriesGroupBy.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_head_tail.py`

### Class: `GroupbyParityHeadTailTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_head`
Tests the head() method on a Spark DataFrame in Spark Connect.

#### `test_tail`
Tests the tail() method on a Spark DataFrame in Spark Connect.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_index.py`

### Class: `GroupbyParityIndexTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_groupby_multiindex_columns`
Tests groupby operations when columns are a MultiIndex.

#### `test_idxmax`
Tests idxmax() on grouped data for finding indices of maximum values.

#### `test_idxmax_idxmin_skipna_false_with_na`
Tests idxmax and idxmin with skipna=False and NaN values, handling pandas version differences.

#### `test_idxmin`
Tests idxmin() on grouped data for finding indices of minimum values.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_missing.py`

### Class: `MissingParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_missing`
Tests that unsupported pandas functions correctly raise NotImplementedError.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_missing_data.py`

### Class: `GroupbyParityMissingDataTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_bfill`
Tests the bfill() (backward fill) operation on grouped data.

#### `test_dropna`
Tests the dropna() method on a Spark DataFrame.

#### `test_ffill`
Tests the ffill() (forward fill) operation on grouped data.

#### `test_fillna`
Tests the fillna() method on a Spark DataFrame.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_nlargest_nsmallest.py`

### Class: `NlargestNsmallestParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_nlargest`
Tests the nlargest() operation on grouped data to find top N values.

#### `test_nsmallest`
Tests the nsmallest() operation on grouped data to find bottom N values.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_raises.py`

### Class: `RaisesParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_raises`
Verifies that groupby raises correct exceptions for invalid inputs.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_rank.py`

### Class: `RankParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_rank`
Tests the rank() operation on grouped data.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_size.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SizeParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_size`
Tests the size() operation on grouped data to count elements per group.

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_split_apply.py`

### Class: `GroupbyParitySplitApplyTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_split_apply_combine_on_series`
Tests split-apply-combine with variance on a series.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_split_apply_count.py`

### Class: `GroupbySplitApplyCountParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_split_apply_combine_on_series`
Tests split-apply-combine with variance on a series in count tests context.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_split_apply_first.py`

### Class: `GroupbySplitApplyFirstParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_split_apply_combine_on_series`
Tests split-apply-combine with variance on a series in first tests context.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_split_apply_last.py`

### Class: `GroupbySplitApplyLastParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_split_apply_combine_on_series`
Tests split-apply-combine with variance on a series in last tests context.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_split_apply_min_max.py`

### Class: `GroupbySplitApplyMMParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_split_apply_combine_on_series`
Tests split-apply-combine with variance on a series in min/max tests context.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_split_apply_skew.py`

### Class: `GroupbySplitApplySkewParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_split_apply_combine_on_series`
Tests split-apply-combine with variance on a series in skew tests context.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_split_apply_std.py`

### Class: `GroupbySplitApplyStdParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_split_apply_combine_on_series`
Tests split-apply-combine with variance on a series in std tests context.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_split_apply_var.py`

### Class: `GroupbySplitApplyVarParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_split_apply_combine_on_series`
Tests split-apply-combine with variance on a series in var tests context.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_stat.py`

### Class: `GroupbyParityStatTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_max`
Tests the max() operation on grouped data.

#### `test_mean`
Tests the mean() operation on grouped data.

#### `test_median`
Verifies that the internal median function is correctly constructed for Spark.

#### `test_min`
Tests the min() operation on grouped data.

#### `test_sum`
Tests the sum() operation on grouped data.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_stat_adv.py`

### Class: `GroupbyStatAdvParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_first`
Tests the first() method on a Spark DataFrame in Spark Connect.

#### `test_last`
Tests the last() operation on grouped data.

#### `test_nth`
Tests the nth() operation on grouped data to get the n-th item.

#### `test_quantile`
Tests the quantile() operation on grouped data.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_stat_ddof.py`

### Class: `GroupbyStatDdofParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_ddof`
Tests std, var, and sem with different delta degrees of freedom on grouped data.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_stat_func.py`

### Class: `GroupbyStatFuncParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_basic_stat_funcs`
Tests basic statistical functions (var, median, std, sem, sum) on grouped data.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_stat_prod.py`

### Class: `GroupbyStatProdParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_prod`
Tests the prod() (product) operation on grouped data.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/groupby/test_parity_value_counts.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `ValueCountsParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_value_counts`
Tests the value_counts() operation on grouped data.

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_align.py`

### Class: `FrameParityAlignTests`

#### `test_align`
Tests the align() method for DataFrames and Series.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_append.py`

### Class: `AppendParityTests`

#### `test_append`
Tests the append() method for indexes, especially CategoricalIndex.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_asof.py`

### Class: `IndexesAsOfParityTests`

#### `test_asof`
Tests the asof() method for regular and datetime indexes.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_astype.py`

### Class: `IndexesAsTypeParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_astype`
Tests the astype method for converting a Series to a categorical type, comparing behavior between pandas and pandas-on-Spark.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_basic.py`

### Class: `IndexBasicParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_factorize`
Tests the factorize method for encoding categorical values, including handling of missing values, comparing pandas and pandas-on-Spark.

#### `test_holds_integer`
Tests the holds_integer method on different index types, verifying it returns correct boolean values or raises AttributeError in newer pandas versions.

#### `test_index_basic`
Tests basic index functionality across various data types and error handling for invalid operations.

#### `test_index_ops`
Tests arithmetic operations on indices and multi-index level values, ensuring parity with pandas.

#### `test_inferred_type`
Tests the inferred_type property for various index types to ensure correct type identification.

#### `test_item`
Tests the item method for retrieving a single element from an index, including error handling for non-singleton indices.

#### `test_multi_index_copy`
Tests the copy method on a MultiIndex to ensure correct duplication.

#### `test_view`
Tests the view method on both single and multi-indices for creating a new view of the data.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_category.py`

### Class: `CategoricalIndexParityTests`

#### `test_add_categories`
Tests adding categories to a categorical index, including error handling for duplicate categories.

#### `test_append`
Tests appending categorical indices, verifying successful operations and expected NotImplementedError for unsupported cases.

#### `test_as_ordered_unordered`
Tests converting a categorical index to ordered and unordered states.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_astype`
Tests the astype method for converting a Series to a categorical type.

#### `test_categorical_index`
Comprehensive tests for creating and manipulating categorical indices, including verification of categories, codes, and ordering.

#### `test_categories_setter`
Tests renaming categories and validating error conditions when setting an incorrect number of categories.

#### `test_factorize`
Tests the factorize method for encoding categorical values, including handling of missing values.

#### `test_insert`
Tests inserting a value into a CategoricalIndex.

#### `test_intersection`
Extensive tests for the intersection method of indices against various other types, including error handling.

#### `test_map`
Tests the map method on indices using dictionaries, lambda functions, and Series.

#### `test_remove_categories`
Tests removing categories from a categorical index, including error handling for non-existent categories.

#### `test_remove_unused_categories`
Tests removing unused categories from a categorical index.

#### `test_rename_categories`
Tests renaming categories using lists, dictionaries, and callables, with error checking for invalid inputs.

#### `test_reorder_categories`
Tests reordering categories in a categorical index, with error checking for invalid inputs.

#### `test_set_categories`
Tests setting categories in a categorical index, including renaming and ordering options, with error checking.

#### `test_union`
Tests Spark Connect DataFrame union and unionByName operations, verifying the generated proto plans.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_conversion.py`

### Class: `ConversionParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_index_from_index`
Tests creating a new index from an existing index, verifying dtype, name, and copy parameters.

#### `test_index_from_series`
Tests creating an index from a Series, including datetime handling.

#### `test_multi_index_from_index`
Tests creating a MultiIndex from an index of tuples, including setting level names.

#### `test_multiindex_from_arrays`
Tests creating a MultiIndex from arrays.

#### `test_multiindex_from_frame`
Extensive tests for creating a MultiIndex from a DataFrame, covering various edge cases and parameter configurations.

#### `test_multiindex_from_product`
Tests creating a MultiIndex from the cartesian product of iterables.

#### `test_multiindex_from_tuples`
Tests creating a MultiIndex from a list of tuples.

#### `test_to_frame`
Tests the to_frame method on indices, converting them to DataFrames with various naming options.

#### `test_to_list`
Tests the tolist method on both single and multi-indices to convert them to Python lists.

#### `test_to_numpy`
Tests the to_numpy method on an index to convert it to a NumPy array.

#### `test_to_series`
Extensive tests for the to_series method on indices, converting them to Series with various options.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_datetime.py`

### Class: `DatetimeIndexParityTests`

#### `test_arithmetic_op_exceptions`
Verifies that unsupported arithmetic operations on a DatetimeIndex correctly raise TypeErrors.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_datetime_index`
Tests error handling for invalid operations on a DatetimeIndex.

#### `test_day_name`
Tests the day_name method on a DatetimeIndex.

#### `test_month_name`
Tests the month_name method on a DatetimeIndex.

#### `test_normalize`
Tests the normalize method on a DatetimeIndex to reset time to midnight.

#### `test_strftime`
Tests the strftime method on a DatetimeIndex for date formatting.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_datetime_at.py`

### Class: `DatetimeIndexAtParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_indexer_at_time`
Tests the indexer_at_time method on a DatetimeIndex to locate values at specific times.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_datetime_between.py`

### Class: `DatetimeIndexBetweenParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_indexer_between_time`
Tests the indexer_between_time method on a DatetimeIndex to select values within specific time ranges.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_datetime_ceil.py`

### Class: `DatetimeIndexCeilParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_ceil`
Tests the ceil method on a DatetimeIndex to round up to specified frequencies.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_datetime_floor.py`

### Class: `DatetimeIndexFloorParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_floor`
Tests the floor method on a DatetimeIndex to round down to specified frequencies.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_datetime_iso.py`

### Class: `DatetimeIndexISOParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_isocalendar`
Tests the isocalendar method on a DatetimeIndex, verifying year, week, and day.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_datetime_map.py`

### Class: `DatetimeIndexMapParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_map`
Tests the map method on a DatetimeIndex using dictionaries, lambda functions, and Series.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_datetime_property.py`

### Class: `DatetimeIndexParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_properties`
Tests the properties of SparkConnectClient, specifically token and host parsing from the connection string.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_datetime_round.py`

### Class: `DatetimeIndexRoundParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_round`
Tests the round method on a DatetimeIndex to round to specified frequencies.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_default.py`

### Class: `DefaultIndexParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_default_index_distributed`
Verifies that the 'distributed' default index type produces unique index values for a large DataFrame.

#### `test_default_index_distributed_sequence`
Verifies that the 'distributed-sequence' default index type produces a continuous sequence of integers as indices.

#### `test_default_index_sequence`
Verifies that the 'sequence' default index type produces a continuous sequence of integers as indices.

#### `test_index_distributed_sequence_cleanup`
Calls the superclass cleanup method for the distributed sequence index test.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_delete.py`

### Class: `IndexesDeleteParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_delete`
Tests the deletion of elements from single and multi-level indices by index position, ensuring parity with Pandas and proper error handling for out-of-bounds indices.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_diff.py`

### Class: `IndexesDiffParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_difference`
Verifies the behavior of the difference method for both single and multi-indices against various iterable types, ensuring correct results and error handling for invalid inputs.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_drop.py`

### Class: `IndexesDropParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_drop_duplicates`
Tests the removal of duplicate rows in a Spark DataFrame with specific subsets, and ensures proper type checking and error messages for invalid inputs.

#### `test_drop_level`
Verifies that an IndexError is raised when trying to drop a non-existent level from a multi-index.

#### `test_dropna`
Validates the dropna functionality in a Spark DataFrame across multiple parameters such as 'how', 'thresh', and 'subset', including error validation for invalid input types.

#### `test_index_drop`
Tests dropping specific labels from a single-level index, comparing behavior with Pandas.

#### `test_index_drop_duplicates`
Verifies that dropping duplicate values from both single and multi-level indices matches Pandas behavior after sorting.

#### `test_multiindex_drop`
Tests dropping labels from a multi-index by label or level (by index or name), verifying error handling for invalid levels or keys.

#### `test_multiindex_droplevel`
Tests dropping levels from a multi-index by index position or name, including non-string names, and ensures appropriate error handling for invalid operations.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_getattr.py`

### Class: `IndexGetattrParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_index_getattr`
Verifies that accessing an invalid attribute on an Index or DatetimeIndex correctly raises an AttributeError.

#### `test_multi_index_getattr`
Verifies that accessing an invalid attribute on a MultiIndex correctly raises an AttributeError.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_indexing.py`

### Class: `FrameParityIndexingTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_getitem_with_none_key`
Confirms that indexing a DataFrame with None raises a KeyError.

#### `test_head`
Tests the head method on a Spark DataFrame with varying row counts, including empty result sets.

#### `test_insert`
Tests inserting a value into a CategoricalIndex, comparing with Pandas behavior.

#### `test_items`
Verifies that the items iterator on a DataFrame yields the correct column names and content, matching Pandas.

#### `test_iter_dataframe`
Tests the direct iteration over a DataFrame to ensure it yields the expected elements.

#### `test_iterrows`
Verifies that the iterrows method on a DataFrame yields the correct index and row data for both single and multi-indices.

#### `test_itertuples`
Validates itertuples across various configurations, including non-default names, excluded indices, multi-indices, and a high volume of columns.

#### `test_keys`
Tests that the keys method on a DataFrame correctly returns the column names.

#### `test_mask`
Ensures that the mask method raises a TypeError when passed an invalid condition type.

#### `test_multiindex_column_access`
Tests accessing columns in a DataFrame with a MultiIndex column structure using various indexing formats.

#### `test_query`
Verifies the query method with various filters and the inplace parameter, including type check error handling for inputs and MultiIndex column limitations.

#### `test_tail`
Tests the tail method on a Spark DataFrame, comparing it to another Spark DataFrame's output.

#### `test_where`
Verifies that the where method on a Spark DataFrame enforces type checking for its condition argument.

#### `test_xs`
Tests the xs method for extracting a cross-section from a multi-indexed DataFrame by label or level, and checks for proper error handling.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_indexing_adv.py`

### Class: `IndexingAdvParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_at`
Tests label-based scalar lookup using the .at accessor for DataFrames and Series, verifying error conditions and that setting values is not supported.

#### `test_at_multiindex`
Tests label-based scalar lookup using .at on a DataFrame with a multi-level index.

#### `test_at_multiindex_columns`
Tests label-based scalar lookup using .at on a DataFrame with a MultiIndex column structure.

#### `test_getitem`
Verifies that using __getitem__ on a DataFrame followed by groupby returns a SeriesGroupBy object.

#### `test_getitem_period_str`
Tests date/time partial string slicing on a DataFrame with a PeriodIndex.

#### `test_getitem_slice`
Tests label-based slicing of a DataFrame.

#### `test_getitem_timestamp_str`
Tests date/time partial string slicing on a DataFrame with a DatetimeIndex.

#### `test_iat`
Tests integer position-based scalar lookup using .iat on DataFrames and Series, verifying error conditions and that setting values is not supported.

#### `test_iat_multiindex`
Tests integer position-based scalar lookup using .iat on a DataFrame with a MultiIndex.

#### `test_iat_multiindex_columns`
Tests integer position-based scalar lookup using .iat on a DataFrame with MultiIndex columns.

#### `test_iloc`
Tests integer position-based indexing and slicing using .iloc on a DataFrame, ensuring it enforces numeric slices and proper error handling.

#### `test_index_operator_datetime`
Tests both positional and label-based slicing and indexing on a DataFrame with a DatetimeIndex.

#### `test_index_operator_int`
Tests both positional and label-based slicing and indexing on a DataFrame with an integer index.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_indexing_basic.py`

### Class: `BasicIndexingParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_from_pandas_with_explicit_index`
Tests creating a Pandas-on-Spark DataFrame from a Pandas DataFrame with explicit indices.

#### `test_indexing`
A comprehensive test for set_index and reset_index behaviors across various arguments, index depths, column structures, and input types.

#### `test_limitations`
Verifies that reset_index raises a ValueError for invalid mixed-type level inputs.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_indexing_iloc.py`

### Class: `IndexingILocParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_frame_iloc_setitem`
Tests setting values in a DataFrame by integer position using .iloc, verifying error conditions for shape mismatches and sequence assignments.

#### `test_iloc`
Tests integer position-based indexing and slicing using .iloc with various indexer types.

#### `test_iloc_iterable_rows_sel`
Tests row selection using .iloc with various iterable types and negative indices.

#### `test_iloc_multiindex_columns`
Tests integer position-based indexing and slicing using .iloc on a DataFrame with MultiIndex columns.

#### `test_iloc_raises`
Verifies that the .iloc accessor raises appropriate errors for out-of-bounds access, excessive indexers, or invalid slice types.

#### `test_iloc_series`
Verifies that .iloc indexing on a Series behaves identically in Spark Connect as in standard pandas, covering direct indexing, slicing, and indexing on a modified series.

#### `test_iloc_slice_rows_sel`
Tests row selection using .iloc slices on DataFrames and Series, comparing against pandas after sorting results to ensure parity.

#### `test_series_iloc_setitem`
Validates assignment via .iloc on a Series using various indexers and checks that proper exceptions are raised for invalid operations.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_indexing_loc.py`

### Class: `IndexingLocParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_frame_loc_setitem`
Tests item assignment via .loc on DataFrames, covering multi-index columns and verification of edge cases and exceptions for incompatible shapes or indexers.

#### `test_loc`
Comprehensive test for .loc label-based indexing on DataFrames and Series, supporting range slices, list-like selections, and verifying behavior on monotonic and non-monotonic indexes.

#### `test_loc_datetime_no_freq`
Ensures that .loc slicing functions correctly with a DatetimeIndex that does not specify a frequency.

#### `test_loc_getitem_boolean_series`
Validates that .loc correctly filters a DataFrame or Series when provided with a boolean Series as the selector.

#### `test_loc_noindex`
Checks that .loc operations remain consistent after a DataFrame's index has been reset to default integer values.

#### `test_loc_non_informative_index`
Tests that .loc handles slices properly on indexes with duplicates or specific gaps between labels.

#### `test_loc_on_numpy_datetimes`
Tests that label slicing via .loc works correctly when using NumPy datetime64 types as the index.

#### `test_loc_on_pandas_datetimes`
Confirms that label slicing via .loc operates correctly with pandas Timestamp objects as the index.

#### `test_loc_timestamp_str`
Verifies partial string slicing on a DatetimeIndex using .loc, matching behavior against pandas for ranges and specific dates.

#### `test_loc_with_series`
Validates that .loc correctly accepts a boolean Series (e.g., condition on a column) as a row selector for DataFrames and Series.

#### `test_series_loc_setitem`
Asserts that item assignment through .loc works on a Series with various key types including slices and boolean conditions, and covering MultiIndex scenarios.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_indexing_loc_2d.py`

### Class: `IndexingLoc2DParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_loc2d`
Tests 2-dimensional .loc usage for simultaneous row and column selection, asserting correct behavior with list-likes, slices, and boolean masks, including handling non-string columns.

#### `test_loc2d_duplicated_columns`
Tests that .loc handles 2D selection correctly when duplicate column names are present, falling back to pandas parity behavior.

#### `test_loc2d_multiindex`
Verifies 2D selection with .loc on a DataFrame that possesses a MultiIndex.

#### `test_loc2d_multiindex_columns`
Extensive tests for .loc selections on a DataFrame with MultiIndex columns, validating edge cases, type checks, and proper error triggers.

#### `test_loc2d_with_known_divisions`
Verifies that 2D selections with .loc operate correctly on a DataFrame with a known, sorted index.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_indexing_loc_multi_idx.py`

### Class: `IndexingLocMultiIdxParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_loc_multiindex`
Asserts full functional coverage for .loc selections on MultiIndex structures, detailing expected parity on monotonic and correctly failing on non-monotonic index operations.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_insert.py`

### Class: `IndexesInsertParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_insert`
Simple test ensuring that inserting a new category value into a CategoricalIndex yields equivalent results to pandas.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_intersection.py`

### Class: `IntersectionParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_intersection`
Verifies the intersection set operation between indexes and other collection types (like lists and tuples), including MultiIndex cases and error paths.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_level.py`

### Class: `LevelParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_index_get_level_number`
Validates mapping from level names or relative integers back to positive level numbers, confirming specific error patterns for invalid inputs.

#### `test_index_get_level_values`
Verifies that retrieval of level values by index level name or position functions similarly to pandas.

#### `test_index_nlevels`
Confirms that nlevels property correctly reports 1 for a standard, non-multi-index data structure.

#### `test_multi_index_levshape`
Ensures that the dimensions of level lengths match between pandas and Spark Connect for a MultiIndex.

#### `test_multiindex_equal_levels`
Asserts identical behavior to pandas for testing whether levels between two MultiIndexes are equivalent.

#### `test_multiindex_get_level_values`
Confirms get_level_values yields matching output when requested by position or name on MultiIndexes.

#### `test_multiindex_nlevel`
Asserts that a MultiIndex with 2 levels correctly reports its nlevels property as 2.

#### `test_multiindex_swaplevel`
Confirms swaplevel exchanges index tiers by name or position appropriately, including edge checks on boundary exceptions.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_map.py`

### Class: `IndexesMapParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_map`
Verifies map operation using dictionaries, lambda functions, and mapping Series over elements of an Index, verifying correct type coercion and error handling.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_missing.py`

### Class: `MissingParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_missing`
Validates that missing or unsupported pandas methods raise a PandasNotImplementedError when called.

#### `test_multi_index_not_supported`
Validates that specific operations (any, all) that are unsupported for MultiIndex correctly throw a TypeError.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_monotonic.py`

### Class: `MonotonicParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_monotonic`
Heavily assesses monotonicity attributes of complex MultiIndexes containing integers, strings, negatives, booleans, duplicates, and varied null representations.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_name.py`

### Class: `IndexNameParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_index_names`
Tests name assignments on DataFrame indexes, verifying correct behavior with hashables, proper reporting of the error conditions on invalid assignment attempts.

#### `test_multi_index_names`
Validates naming arrays for MultiIndexes, checking exceptions when trying to apply single names to multi-tiered indexes.

#### `test_multiindex_set_names`
Validates that .set_names on a MultiIndex correctly applies names across all levels or to specific targeted levels.

#### `test_multiindex_tuple_column_name`
Confirms behavior when a DataFrame column is promoted to the index where that column had a tuple label as part of a MultiIndex column.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_reindex.py`

### Class: `FrameParityReindexTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_reindex`
Asserts high precision alignment on full .reindex capabilities covering axes transitions, default values, tuple key resolution, and exception validation for bad configurations.

#### `test_reindex_like`
Tests that reindexing using the layout (axes) of another DataFrame functions as expected under multiple Index and MultiIndex scenarios.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_rename.py`

### Class: `FrameParityRenameTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_index_rename`
Confirms that .rename shifts the label name on an Index structure, preventing invalid assignments like lists to single labels.

#### `test_multi_index_rename`
Validates that .rename successfully transitions lists of names onto MultiIndexes, checking invalid operations like single string assignments.

#### `test_multiindex_rename`
A variation that specifically forces sequence assignments over MultiIndex tuples.

#### `test_rename_axis`
Extensively covers rename_axis operations across index and column headers using dictionary lookups, functions, and standard sequence inputs.

#### `test_rename_dataframe`
Asserts correct parity behaviors when executing rename mappings across both index axis and columns axis of a DataFrame, accepting lambda, dicts and standard strings.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_repeat.py`

### Class: `RepeatParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_repeat`
This test case is a placeholder with no implementation.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_reset_index.py`

### Class: `FrameParityResetIndexTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_index_to_frame_reset_index`
Verifies that after casting an index to a DataFrame, normal reset_index behaviors are kept.

#### `test_reset_index`
Tests index promotion and dropping via reset_index, including handling of name collisions and verify proper object mutations.

#### `test_reset_index_with_default_index_types`
Validates that changing Spark Connect default index types maintains appropriate behavior during reset_index steps.

#### `test_reset_index_with_multiindex_columns`
Extensive tests for reset_index behavior when multi-tiered columns are present, detailing column promotion rules.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_sort.py`

### Class: `IndexesSortParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_index_sort`
Verifies that calling sort() on an Index or MultiIndex raises a TypeError advising to use sort_values instead, as in-place sorting is not allowed.

#### `test_sort_values`
Verifies sort_values for both Index and MultiIndex by comparing results against pandas, including cases with named indexes.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_stat.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `StatParityTests`

#### `test_abs`
Verifies the abs function and np.abs for Index by comparing with pandas, and asserts that calling it on a MultiIndex raises a TypeError.

#### `test_argmax`
Verifies argmax for Index against pandas and asserts that it raises a TypeError for MultiIndex.

#### `test_argmin`
Verifies argmin for Index against pandas and asserts that it raises a TypeError for MultiIndex.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_hasnans`
Verifies the hasnans property for Index and Series of various types (Boolean, Timestamp, empty) against pandas, and asserts it raises NotImplementedError for MultiIndex.

#### `test_index_fillna`
Verifies fillna for Index against pandas and asserts that passing a list to it raises a TypeError.

#### `test_len`
Verifies the length (len) of both Index and MultiIndex by comparing with pandas.

#### `test_max`
Verifies the max statistical function on a groupby object with various arguments.

#### `test_min`
Verifies the min statistical function on a groupby object with various arguments.

#### `test_multiindex_isna`
Verifies that calling isna, isnull, notna, and notnull on a MultiIndex raises a NotImplementedError.

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_symmetric_diff.py`

### Class: `IndexesSymmetricDiffParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_index_symmetric_difference`
Verifies symmetric_difference and the ^ operator for Index and MultiIndex against pandas, including edge cases, and asserts NotImplementedError for mixed types.

#### `test_multi_index_symmetric_difference`
Verifies symmetric_difference between two MultiIndex objects against pandas, and asserts NotImplementedError when comparing a MultiIndex with an Index.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_take.py`

### Class: `IndexesTakeParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_take`
Verifies the take API on a Spark Connect DataFrame to ensure it returns the correct number of rows.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_timedelta.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `TimedeltaIndexParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_properties`
Verifies the properties (host and token) of a SparkConnectClient created with different connection strings.

#### `test_timedelta_index`
Verifies creation of TimedeltaIndex from various sources against pandas and tests error cases regarding name hashability and unsupported methods like all.

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_union.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `UnionParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_union`
Verifies that DataFrame union and unionByName operations generate the expected Spark Connect protobuf plans.

## File: `python/pyspark/pandas/tests/connect/indexes/test_parity_unique.py`

### Class: `IndexesUniqueParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_index_has_duplicates`
Verifies the has_duplicates property on Index with various datasets.

#### `test_index_is_unique`
Verifies the is_unique property on Index with various datasets.

#### `test_index_nunique`
Verifies nunique for Index against pandas, checking the result with and without dropping NaNs.

#### `test_index_unique`
Verifies unique for Index, comparing sorted results with expected values, and checks error cases for invalid requested levels.

#### `test_multi_index_nunique`
Verifies that calling nunique on a MultiIndex raises a NotImplementedError.

#### `test_multiindex_has_duplicates`
Verifies the has_duplicates property on MultiIndex with various datasets.

#### `test_multiindex_is_unique`
Verifies the is_unique property on MultiIndex with various datasets.

#### `test_multiindex_nunique`
Verifies that calling notnull on a MultiIndex raises a NotImplementedError (despite test name suggesting nunique).

#### `test_unique`
Verifies unique on a Series groupby object with different data types, comparing sorted results with pandas.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/io/test_parity_csv.py`

### Class: `CsvParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_read_csv`
Verifies that `read_csv` correctly handles various parameters such as `header`, `names`, `usecols`, and `index_col` by comparing the results between pandas-on-Spark and native pandas. It also checks for proper error handling (ValueErrors) when invalid arguments are provided.

#### `test_read_csv_with_comment`
Tests the `comment` parameter in `read_csv`, ensuring that single-character comments are correctly ignored and that appropriate `ValueError` exceptions are raised for invalid comment settings (e.g., multi-character or non-string types).

#### `test_read_csv_with_dtype`
Validates that `read_csv` correctly applies data types specified via the `dtype` parameter, supporting both a single type for all columns and a dictionary mapping specific columns to types.

#### `test_read_csv_with_encoding`
Verifies that `read_csv` correctly handles different file encodings, specifically testing `latin-1` support.

#### `test_read_csv_with_escapechar`
Tests the `escapechar` parameter in `read_csv` to ensure that escaped characters in the CSV data are correctly processed.

#### `test_read_csv_with_limit`
Confirms that the `nrows` parameter in `read_csv` correctly limits the number of rows read from the CSV file.

#### `test_read_csv_with_parse_dates`
Ensures that `read_csv` raises a `ValueError` when the unsupported `parse_dates` parameter is set to `True`.

#### `test_read_csv_with_quotechar`
Validates that the `quotechar` parameter in `read_csv` correctly identifies and handles quoted strings within the CSV file.

#### `test_read_csv_with_sep`
Tests the `sep` (separator) parameter in `read_csv`, ensuring that non-comma delimiters (like tabs) are correctly recognized.

#### `test_read_with_spark_schema`
Verifies that `read_csv` can accept a Spark-style schema string for the `names` parameter to define column names and types.

#### `test_to_csv`
Comprehensive test for the `to_csv` method, verifying output consistency with pandas for both DataFrames and Series. It covers basic export, column selection, handling of null values (`na_rep`), header suppression, and proper exception raising for invalid column specifications.

#### `test_to_csv_with_partition_cols`
Verifies that `to_csv` correctly partitions the output files by specified columns when saving to a directory, matching the expected directory structure and content for each partition.

#### `test_to_csv_with_path`
Tests saving a DataFrame to a CSV file at a specific path, verifying the content against pandas output and checking for correct error handling with invalid column arguments.

#### `test_to_csv_with_path_and_basic_options`
Verifies that `to_csv` correctly applies basic options like `sep`, `header`, and `columns` when saving to a path.

#### `test_to_csv_with_path_and_basic_options_multiindex_columns`
Tests `to_csv` with MultiIndex columns, ensuring it handles column selection and header overrides correctly while raising `ValueError` for unsupported MultiIndex export configurations.

#### `test_to_csv_with_path_and_pyspark_options`
Verifies that Spark-specific options (like `nullValue`) passed to `to_csv` are correctly handled and produce the expected output.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/io/test_parity_dataframe_conversion.py`

### Class: `DataFrameConversionParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_from_records`
Validates the `from_records` constructor, ensuring it correctly creates DataFrames from various inputs (dicts, list of tuples, NumPy arrays) and respects parameters like `index`, `exclude`, `columns`, and `nrows`.

#### `test_read_json_negative`
Verifies that `read_json` raises a `NotImplementedError` when called with `lines=False`, as this mode is currently unsupported in pandas-on-Spark.

#### `test_to_clipboard`
Tests the `to_clipboard` method for Series, ensuring parity with pandas for basic usage and options like `excel`, `sep`, and `index`.

#### `test_to_excel`
Comprehensive test for the `to_excel` method, verifying parity with pandas for DataFrames and Series across various options including `na_rep`, `float_format`, `header`, and `index`.

#### `test_to_html`
Verifies that psdf.to_html() correctly converts a pandas-on-Spark DataFrame to an HTML table string, matching the expected HTML structure both with and without the max_rows parameter.

#### `test_to_json`
A placeholder test for to_json parity that currently contains no execution logic.

#### `test_to_json_negative`
Checks that psdf.to_json() correctly raises NotImplementedError for unsupported parameters like orient="table" or lines=False.

#### `test_to_json_with_partition_cols`
Validates that psdf.to_json() correctly writes a DataFrame to JSON files partitioned by a specified column, and verifies the content of the generated partition files.

#### `test_to_json_with_path`
Verifies that psdf.to_json() successfully writes a DataFrame to a specified directory and that the resulting JSON file content matches the expected output.

#### `test_to_latex`
Ensures that psser.to_latex() for a pandas-on-Spark Series produces the same LaTeX representation as a standard pandas Series across various parameters.

#### `test_to_records`
Checks that psdf.to_records() correctly converts a pandas-on-Spark DataFrame into a numpy record array, matching the output of a standard pandas DataFrame.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/io/test_parity_dataframe_spark_io.py`

### Class: `DataFrameSparkIOParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_orc_write`
Tests the to_orc method for writing a pandas-on-Spark DataFrame to ORC format, including partitioning support and passing additional options like compression.

#### `test_parquet_read`
Validates ps.read_parquet() for reading Parquet files, ensuring it correctly handles column selection, index columns, and matches pandas output.

#### `test_parquet_read_with_pandas_metadata`
Verifies that ps.read_parquet() correctly handles Parquet files that contain pandas-specific metadata, ensuring index information is preserved.

#### `test_parquet_write`
Tests the to_parquet method for writing a pandas-on-Spark DataFrame to Parquet format, including partitioning and compression settings.

#### `test_read_excel`
Comprehensively tests ps.read_excel() for reading Excel files from paths or file-like objects, supporting index columns, multiple sheets, and directory-based reading.

#### `test_read_large_excel`
Specifically tests ps.read_excel() with a larger dataset (20,000 rows) to ensure stability and correctness for more substantial Excel files.

#### `test_read_orc`
Validates ps.read_orc() for reading ORC files, verifying support for column selection, indices, and ensuring proper error handling for invalid columns.

#### `test_spark_io`
Tests general Spark I/O capabilities via ps.read_spark_io and psdf.spark.to_spark_io, specifically verifying partitioning and index handling for JSON format.

#### `test_table`
Verifies that self.spark.table(None) correctly raises a PySparkTypeError when an invalid table name is provided.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/io/test_parity_feather.py`

### Class: `FeatherParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_to_feather`
Validates psdf.to_feather() by writing a pandas-on-Spark DataFrame to Feather format and verifying parity with standard pandas output.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/io/test_parity_io.py`

### Class: `FrameParityIOTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_from_dict`
Ensures that ps.DataFrame.from_dict() correctly creates a pandas-on-Spark DataFrame from a dictionary, matching standard pandas behavior.

#### `test_info`
Verifies that psdf.info() produces diagnostic information that matches standard pandas df.info(), supporting various formatting parameters.

#### `test_style`
Performs minimal validation of psdf.style, ensuring it can apply styles and produce a LaTeX representation consistent with pandas Styler objects.

#### `test_to_markdown`
Tests that the to_markdown() method of a pandas-on-Spark DataFrame produces the same markdown string as a regular pandas DataFrame.

#### `test_to_numpy`
Verifies that converting a pandas-on-Spark Index to a NumPy array using to_numpy(copy=True) yields the same result as converting a standard pandas Index.

#### `test_to_pandas`
Compares the toPandas() results of SQL queries executed via Spark Connect and traditional Spark, ensuring parity across various data types including booleans, integers, floats, and nulls.

#### `test_to_spark`
Tests error handling in the to_spark() method of a pandas-on-Spark DataFrame, specifically ensuring it raises ValueError for overlapping index columns or mismatched index lengths.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/io/test_parity_series_conversion.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesConversionParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_to_clipboard`
Checks for parity between pandas-on-Spark Series and pandas Series for the to_clipboard() method, testing different parameters like excel, sep, and index.

#### `test_to_latex`
Validates that the to_latex() method of a pandas-on-Spark Series produces identical LaTeX output to a pandas Series across a variety of formatting options.

## File: `python/pyspark/pandas/tests/connect/io/test_parity_stata.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `StataParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_to_feather`
Tests the to_feather() method by saving both a pandas DataFrame and a pandas-on-Spark DataFrame to Feather files and verifying that reading them back results in identical data.

## File: `python/pyspark/pandas/tests/connect/plot/test_parity_frame_plot.py`

### Class: `DataFramePlotParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_compute_box`
Verifies the internal compute_box logic used for generating box plots in pandas-on-Spark, ensuring it correctly calculates statistics like quantiles and outliers for multiple columns.

#### `test_compute_hist_multi_columns`
Tests the calculation of histogram bins and frequency counts for multi-column pandas-on-Spark DataFrames, comparing the results against expected NumPy arrays.

#### `test_compute_hist_single_column`
Tests the calculation of histogram bins and frequency counts for a single-column pandas-on-Spark DataFrame.

#### `test_missing`
Programmatically iterates through a list of unsupported pandas functions in the pandas-on-Spark namespace and verifies that attempting to call them raises a PandasNotImplementedError.

#### `test_sampled_plot_with_max_rows`
Validates that the sampling mechanism for PySpark plotting correctly reduces the dataset size to approximately the expected ratio when a large number of rows are present.

#### `test_sampled_plot_with_ratio`
Tests that setting the plotting.sample_ratio option correctly influences the number of rows sampled from a pandas-on-Spark DataFrame for plotting purposes.

#### `test_topn_max_rows`
Verifies that the spark.sql.pyspark.plotting.max_rows configuration correctly limits the number of rows returned by the "top-N" plotting strategy.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/plot/test_parity_frame_plot_matplotlib.py`

### Class: `DataFramePlotMatplotlibParityTests`

#### `test_area_plot`
Tests the generation of area plots from Spark DataFrames, verifying the structure and data content of the resulting figure for both single and multiple column Y-axes.

#### `test_area_plot_stacked_false`
Ensures that unstacked area plots (where stacked=False) generated by pandas-on-Spark are identical to those generated by pandas, including for multi-index columns.

#### `test_area_plot_y`
Verifies that area plots created with a specifically designated y column in pandas-on-Spark match the output of standard pandas.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_bar_plot`
Tests the generation of bar plots from Spark DataFrames, checking that the resulting figure data correctly represents the categories and values for both single and multiple value columns.

#### `test_bar_with_x_y`
Confirms that bar plots created by specifying both x and y column names in pandas-on-Spark yield results identical to pandas.

#### `test_barh_plot`
Validates the generation of horizontal bar plots (barh) from Spark DataFrames, ensuring correct orientation and data mapping for various combinations of X and Y column specifications.

#### `test_barh_plot_with_x_y`
Verifies that horizontal bar plots (barh) correctly handle cases where x and y columns are explicitly specified, ensuring parity between pandas and pandas-on-Spark for both .plot(kind='barh') and .plot.barh() methods.

#### `test_hist_plot`
Validates histogram plots by checking that the generated plot data (bins, counts, and hover text) matches expected values for single and multiple columns.

#### `test_kde_plot`
Tests Kernel Density Estimate (KDE) plots by verifying that the resulting plot data (scatter type with lines) has the correct attributes and shared x-axis values for single and multiple columns.

#### `test_line_plot`
Verifies line plots for single and multiple vertical axes by checking that the category labels (x-axis) and data values (y-axis) in the generated plot data are correct.

#### `test_pie_plot`
Validates pie plots by checking labels and values for single and multiple columns (with subplots), and ensures that correct PySpark errors are raised when required parameters are missing or invalid column types are used.

#### `test_pie_plot_error_message`
Confirms that a ValueError with a specific descriptive message is raised when a pie plot is requested without either a 'y' column or 'subplots=True' being set.

#### `test_scatter_plot`
Verifies scatter plots by checking that the generated plot data contains the correct x and y coordinate values for different column pairings.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/plot/test_parity_frame_plot_plotly.py`

### Class: `DataFramePlotPlotlyParityTests`

#### `test_area_plot`
Validates area plots for single and multiple columns by verifying that the generated plot data (scatter type with lines) contains the correct x-axis dates and y-axis values.

#### `test_area_plot_y`
Ensures parity between pandas and pandas-on-Spark for area plots when a specific 'y' column is provided, verifying that the resulting plots are identical.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_bar_plot`
Checks bar plots for single and multiple vertical axes, verifying that the category labels and values in the generated plot data are correct.

#### `test_bar_with_x_y`
Verifies that bar plots work correctly when x and y columns are explicitly specified, ensuring parity between pandas and pandas-on-Spark outputs.

#### `test_barh_plot`
Validates horizontal bar plots for single and multiple columns on both vertical and horizontal axes, checking that orientation and data in the resulting plot are correct.

#### `test_barh_plot_with_x_y`
Tests horizontal bar plots with explicitly specified x and y columns, ensuring that pandas and pandas-on-Spark produce identical results for both .plot(kind='barh') and .plot.barh() calls.

#### `test_hist_layout_kwargs`
Verifies that layout-related keyword arguments (like 'title') are correctly applied to histogram plots, while ensuring that unrecognized arguments are ignored.

#### `test_hist_plot`
Validates histogram plots by checking that the generated plot data (bins, counts, and hover text) matches expected values for single and multiple columns.

#### `test_kde_plot`
Tests Kernel Density Estimate (KDE) plots by verifying that the resulting plot data (scatter type with lines) has the correct attributes and shared x-axis values for single and multiple columns.

#### `test_line_plot`
Verifies line plots for single and multiple vertical axes by checking that the category labels (x-axis) and data values (y-axis) in the generated plot data are correct.

#### `test_pie_plot`
Validates pie plots by checking labels and values for single and multiple columns (with subplots), and ensures that correct PySpark errors are raised when required parameters are missing or invalid column types are used.

#### `test_scatter_plot`
Verifies scatter plots by checking that the generated plot data contains the correct x and y coordinate values for different column pairings.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/plot/test_parity_series_plot.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesPlotParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_box_summary`
Validates the statistical summary values computed for box plots (mean, median, q1, q3, and fliers) by comparing the results from Spark with expected values derived from pandas.

#### `test_plot_backends`
Verifies that setting the 'plotting.backend' option to 'plotly' correctly sets the pandas-on-Spark plot backend module to 'pyspark.pandas.plot.plotly'.

#### `test_plot_backends_incorrect`
Verifies that setting the 'plotting.backend' option to an invalid module name raises a ValueError when attempting to retrieve the plot backend.

## File: `python/pyspark/pandas/tests/connect/plot/test_parity_series_plot_matplotlib.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesPlotMatplotlibParityTests`

#### `test_area_plot`
Verifies the correctness of generated Plotly area plots for single and multiple columns by checking properties like orientation, type, x/y values, and mode against expected dictionaries.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_bar_plot`
Verifies the correctness of generated Plotly bar plots for single and multiple columns by checking properties like orientation, type, and x/y values against expected dictionaries.

#### `test_bar_plot_limited`
Verifies that bar plot generation in matplotlib using pandas-on-Spark matches the equivalent pandas bar plot visually when rendering a limited number of elements (top 1000).

#### `test_barh_plot`
Verifies the correctness of generated Plotly horizontal bar plots (barh) for single and multiple columns by checking properties like orientation (h), type, and x/y values against expected dictionaries.

#### `test_barh_plot_limited`
Verifies that horizontal bar plot (barh) generation in matplotlib using pandas-on-Spark matches the equivalent pandas plot visually when rendering a limited number of elements (top 1000).

#### `test_box_plot`
Verifies the correctness of generated Plotly box plots for single and multiple columns by comparing statistical aggregates (mean, median, q1, q3, fences) against expected dictionaries, and tests that passing unsupported arguments raises a PySparkValueError.

#### `test_empty_hist`
Verifies that calling `plot.hist()` on a pandas-on-Spark Series containing only non-numeric (categorical) data raises a TypeError indicating there is no numeric data to plot.

#### `test_hist`
Verifies that histogram plot generation in matplotlib using a pandas-on-Spark Series matches the visual output of the equivalent pandas Series histogram plot.

#### `test_hist_plot`
Verifies the correctness of generated Plotly histogram bar plots for single and multiple columns by comparing bin edges, counts, and hover text against expected dictionaries.

#### `test_kde_plot`
Verifies the correctness of generated Plotly KDE (Kernel Density Estimate) plots for single and multiple columns by checking properties like mode, orientation, and type against expected dictionaries, and ensuring x-axis values align between multiple columns.

#### `test_line_plot`
Verifies the correctness of generated Plotly line plots for single and multiple columns by checking properties like orientation, type, x/y values, and mode against expected dictionaries.

#### `test_pie_plot`
Verifies the correctness of generated Plotly pie plots for single and multiple numerical columns by comparing labels and values against expected dictionaries, and ensures that appropriate PySparkValueError or PySparkTypeError exceptions are raised for unsupported parameters or invalid column types.

#### `test_pie_plot_limited`
Verifies that pie plot generation in matplotlib using pandas-on-Spark matches the equivalent pandas pie plot visually when rendering a limited number of elements (top 1000).

#### `test_single_value_hist`
Verifies that histogram plot generation in matplotlib for a pandas-on-Spark Series with a single constant value matches the visual output of the equivalent pandas Series.

## File: `python/pyspark/pandas/tests/connect/plot/test_parity_series_plot_plotly.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesPlotPlotlyParityTests`

#### `test_area_plot`
Verifies the correctness of generated Plotly area plots for single and multiple columns by checking properties like orientation, type, x/y values, and mode against expected dictionaries.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_bar_plot`
Verifies the correctness of generated Plotly bar plots for single and multiple columns by checking properties like orientation, type, and x/y values against expected dictionaries.

#### `test_barh_plot`
Verifies the correctness of generated Plotly horizontal bar plots (barh) for single and multiple columns by checking properties like orientation, type, and x/y values against expected dictionaries.

#### `test_hist_plot`
Verifies the correctness of generated Plotly histogram bar plots for single and multiple columns by comparing bin edges, counts, and hover text against expected dictionaries.

#### `test_kde_plot`
Verifies the creation of Kernel Density Estimate (KDE) plots using Plotly. It checks single column, multiple columns, and all columns plots, validating the generated figure data (mode, name, orientation, type) and ensuring x-axis consistency between different columns.

#### `test_line_plot`
Tests line plot generation with Plotly for both single and multiple vertical axes. It validates the figure's data structure, including labels, values, and plot types for each series.

#### `test_pie_plot`
Validates pie plot generation using Plotly. It checks single column plots, multiple numeric columns with subplots, and ensures that appropriate errors are raised when subplots is not specified for multiple columns or when a non-numeric column is used for the 'y' axis.

#### `test_pox_plot`
Verifies box plot (referred to as "pox" in the test name) generation with Plotly for pandas-on-Spark Series. It compares the dictionary representation of the generated figure against an expected Plotly figure, checking statistics like quartiles, median, mean, and outliers for both simple and MultiIndex columns.

#### `test_pox_plot_arguments`
Tests the validation of box plot arguments in Plotly. It ensures that unsupported arguments like boxpoints="all" and notched=True raise a ValueError, while supported arguments like hovertext are accepted.

## File: `python/pyspark/pandas/tests/connect/resample/test_parity_error.py`

### Class: `ResampleParityErrorTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_resample_error`
Comprehensive error handling test for the resample method. It verifies that appropriate errors (NotImplementedError or ValueError) are raised for non-datetime indexes, unsupported rule codes, non-positive offsets, invalid closed or label parameters, unsupported on types, and cases with no available aggregation columns.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/resample/test_parity_frame.py`

### Class: `ResampleParityFrameTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_dataframe_resample`
Verifies resample functionality for DataFrames by comparing results between pandas and pandas-on-Spark for various time frequencies (hours, days, minutes, seconds) and aggregation functions (mean, std, var). It also checks for unsupported rule code errors.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/resample/test_parity_missing.py`

### Class: `ResampleParityMissingTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_missing`
Iterates through functions marked as "unsupported" in MissingPandasLikeGeneralFunctions and verifies that calling them from the pyspark.pandas namespace correctly raises a PandasNotImplementedError with a descriptive message.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/resample/test_parity_on.py`

### Class: `ResampleParityOnTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_resample_on`
Tests the resample method when using the on parameter to specify a particular column for resampling instead of the index. It validates the parity between pandas and pandas-on-Spark by comparing the summed results of a 2-day resample on a datetime column.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/resample/test_parity_series.py`

### Class: `ResampleParitySeriesTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_series_resample`
Verifies resample functionality for Series by comparing results between pandas and pandas-on-Spark for various frequencies and aggregations (max, sum, mean, var, std). It also tests for unsupported rule code errors.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/resample/test_parity_timezone.py`

### Class: `ResampleParityTimezoneTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_series_resample_with_timezone`
Tests Series resampling with specific SQL configurations for timezone (Asia/Seoul) and timestamp type (TIMESTAMP_NTZ). It ensures parity between pandas and pandas-on-Spark when resampling under these specific environment settings.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/reshape/test_parity_get_dummies.py`

### Class: `GetDummiesParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_get_dummies`
Tests the get_dummies function for Series and DataFrames, including cases with non-string column names. It validates results against pandas and ensures that appropriate NotImplementedError exceptions are raised for sparse outputs, byte strings, or null values.

#### `test_get_dummies_boolean`
Verifies that get_dummies correctly handles boolean data in both DataFrames and Series, maintaining parity with pandas' output.

#### `test_get_dummies_date_datetime`
Tests get_dummies with date and datetime objects, ensuring that it correctly creates indicator variables for these temporal types in both DataFrames and Series.

#### `test_get_dummies_decimal`
Validates that get_dummies can handle Decimal types, producing identical indicator matrices to pandas for both DataFrames and Series containing decimal values.

#### `test_get_dummies_dtype`
Verifies the dtype parameter in get_dummies, ensuring that the resulting indicator columns are correctly cast to the specified data type (e.g., float64).

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/reshape/test_parity_get_dummies_kwargs.py`

### Class: `GetDummiesKWArgsParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_get_dummies_kwargs`
Tests various keyword arguments for get_dummies including prefix, prefix_sep, drop_first, and dummy_na. It also validates correct handling of NaN values during indicator variable generation.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/reshape/test_parity_get_dummies_multiindex.py`

### Class: `GetDummiesMultiIndexParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_get_dummies_multiindex_columns`
Tests get_dummies with MultiIndex columns, including both string and non-string levels. It verifies that specific columns or levels can be targeted for expansion and checks that appropriate errors (KeyError, ValueError, TypeError) are raised for invalid column specifications.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/reshape/test_parity_get_dummies_object.py`

### Class: `GetDummiesObjectParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_get_dummies_object`
Tests get_dummies with object-type columns (strings) and numeric columns. It verifies that columns can be explicitly targeted for expansion, handles non-string column names correctly, and checks for appropriate error raising when invalid column formats are provided.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/reshape/test_parity_get_dummies_prefix.py`

### Class: `GetDummiesPrefixParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_get_dummies_prefix`
Extensively tests the prefix argument in get_dummies using lists and dictionaries to map columns to prefixes. It validates the parity with pandas and ensures correct error handling for mismatched prefix lengths or unsupported types.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/reshape/test_parity_merge_asof.py`

### Class: `MergeAsOfParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_merge_asof`
Verifies the `merge_asof` functionality in pandas-on-Spark, testing various joining strategies including `on`, `left_on`, `right_on`, and index-based joins. It covers parameters like `by`, `tolerance`, `allow_exact_matches`, and `direction` (forward, backward, nearest), and includes comprehensive error handling for invalid arguments and unsupported multi-index structures.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/series/test_parity_all_any.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesParityAllAnyTests`

#### `test_all`
Tests the `all()` logical reduction method on pandas-on-Spark DataFrames and Series, verifying behavior across different axes (0, 1, 'index', 'columns', None). It validates the `bool_only` parameter, multi-index column support, and `skipna` logic with various null representations (NaN, None).

#### `test_any`
Tests the `any()` logical reduction method on pandas-on-Spark DataFrames and Series, verifying correct behavior across various axes (0, 1, None). It checks the `bool_only` parameter, MultiIndex column support, and `skipna` logic when handling NaN and None values.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/series/test_parity_arg_ops.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesParityArgOpsTests`

#### `test_argmin_argmax`
Verifies the `argmin` and `argmax` methods for pandas-on-Spark Series, testing index retrieval for minimum and maximum values. It covers `skipna` parameter behavior, MultiIndex support, empty Series, and detailed error handling for null-only Series and invalid axis specifications, with version-specific checks for pandas 3.0+ compatibility.

#### `test_argsort`
Tests the `argsort` method for pandas-on-Spark Series, verifying the integer indices that would sort the Series. It covers various scenarios including Series with/without null values, MultiIndex support, named Series, and Series derived from Index or DataFrame objects.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/series/test_parity_as_of.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesParityArgOpsTests`

#### `test_asof`
Verifies the `asof` method for pandas-on-Spark Index and DatetimeIndex objects, testing retrieval of the last value before a given label for both monotonic increasing and decreasing indices. It also ensures appropriate error handling for non-monotonic indices and unsupported MultiIndex structures.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/series/test_parity_as_type.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesParityAsTypeTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_astype`
Tests the `astype` method for pandas-on-Spark Series, specifically focusing on conversion to and between categorical types. It validates parity with pandas for `CategoricalDtype` conversions and string casting from categorical data.

## File: `python/pyspark/pandas/tests/connect/series/test_parity_compute.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesParityComputeTests`

#### `test_abs`
Tests absolute value operations (`abs()` and `np.abs()`) on pandas-on-Spark Index objects, ensuring parity with pandas for numerical indices and verifying that attempting to use `abs` on a MultiIndex correctly raises a TypeError.

#### `test_aggregate`
Extensive test for `aggregate` (and `agg`) methods on grouped DataFrames, covering combinations of `as_index` (True/False), various aggregation functions (sum, min, max), and grouping by column names or Series. It validates complex aggregation scenarios using dictionaries (multi-column) and lists (multi-function), and includes support for MultiIndex columns and non-string column names.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_at_time`
Verifies the `at_time` method for selecting rows at specific times of day in DatetimeIndex-indexed DataFrames. It tests various index/column naming combinations (including labels like 'index') and ensures correct error handling for unsupported arguments (`asof`, `axis=1`) and non-datetime indices.

#### `test_between`
Tests the `between` method for Spark Connect columns, ensuring parity between Connect and traditional Spark SQL. It validates range filtering for various data types including floats, decimals, timestamps, and dates.

#### `test_between_time`
Verifies the `between_time` method for selecting DataFrame rows within a specific time interval. It tests various index and column naming scenarios, validates the `inclusive` parameter options ('left', 'right', 'neither', 'both'), and ensures correct error handling for non-DatetimeIndex data or unsupported axes.

#### `test_clip`
Tests the `clip` method for DataFrames and Series, verifying that values outside a specified range are trimmed. It validates `lower` and `upper` bounds, ensures parity with pandas for numerical data, confirms that list-like bounds are currently unsupported, and checks the no-op behavior on string data.

#### `test_compare`
Verifies the `compare` method for pandas-on-Spark Series, ensuring accurate identification of differences between two Series. It tests `keep_shape` and `keep_equal` parameters, MultiIndex support, and validates error handling for mismatched labels, including a specific check for deferred index validation when `compute.eager_check` is disabled.

#### `test_concat`
Tests the `ps.concat` function for merging DataFrames and Series, verifying parameters like `ignore_index` and `sort`. It validates handling of non-matching columns, MultiIndex data, and ensures appropriate enforcement (or bypassing via `ignore_index`) of matching index level requirements.

#### `test_diff`
Tests the `diff` method within grouped pandas-on-Spark DataFrames and Series, calculating first-order differences between elements. It validates grouping by single/multiple columns, grouping by expressions, and support for MultiIndex columns.

#### `test_drop`
Verifies the `drop` method in the Spark Connect DataFrame API by inspecting the generated logical plan. It ensures that both string-based and Column-object-based drops are correctly translated into the underlying protobuf plan for the Connect server.

#### `test_drop_duplicates`
Tests the `dropDuplicates` method for Spark DataFrames, verifying its ability to remove identical rows based on full records or specific subsets. It includes detailed validation of error handling, ensuring `PySparkTypeError` is raised when the `subset` argument is not a list/tuple or contains non-string values.

#### `test_drop_with_errors`
Tests the `drop` method for DataFrames with a focus on the `errors` parameter ('ignore' vs 'raise'). It verifies correct behavior when attempting to drop non-existent columns or rows, validates MultiIndex column support, and ensures proper exception raising for invalid `errors` values.

#### `test_duplicated`
Verifies the `duplicated` method for identifying redundant rows in DataFrames. It tests the `keep` parameter ('first', 'last', False) and `subset` parameter (single or multiple columns), while ensuring support for MultiIndex (both index and columns) and non-string column names.

#### `test_duplicates`
Verifies the 'drop_duplicates' method in pandas-on-Spark Series, ensuring parity with pandas for various data types and 'keep' parameter options.

#### `test_explode`
Tests Spark SQL 'explode', 'posexplode_outer', and 'explode_outer' functions, verifying their ability to expand collections and handle empty/null entries.

#### `test_factorize`
Validates the 'factorize' method for categorical data in pandas-on-Spark, ensuring it correctly encodes values into numeric codes and handles null values via 'use_na_sentinel'.

#### `test_pop`
Tests the 'pop' method on pandas-on-Spark Series and DataFrames, including multi-indexed data and validating error messages for invalid keys.

#### `test_shift`
Verifies the 'shift' method within 'groupby' operations in pandas-on-Spark, checking for consistency with pandas across various grouping criteria and parameters.

#### `test_truncate`
Tests the 'truncate' method for both rows and columns in pandas-on-Spark DataFrames, verifying handling of sorted vs. unsorted indices and various 'before'/'after' bounds.

#### `test_unstack`
Verifies the 'unstack' method for multi-indexed Series in pandas-on-Spark, ensuring it correctly pivots index levels into columns.

## File: `python/pyspark/pandas/tests/connect/series/test_parity_conversion.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesParityConversionTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_to_datetime`
Extensively tests 'to_datetime' conversion in pandas-on-Spark, covering various input formats like DataFrames, dictionaries, scalars with units, and ensuring correct handling of varied time components.

#### `test_to_frame`
Validates the 'to_frame' method on pandas-on-Spark Indexes, checking for parity with pandas for single and multi-indexes, and verifying correct handling of column names.

#### `test_to_list`
Checks the 'tolist' method on pandas-on-Spark Indexes and MultiIndexes to ensure it correctly converts index values into a Python list.

#### `test_to_markdown`
Verifies that 'to_markdown' produces identical markdown table strings for pandas and pandas-on-Spark DataFrames.

#### `test_to_numpy`
Ensures 'to_numpy' correctly converts pandas-on-Spark Indexes into NumPy arrays.

## File: `python/pyspark/pandas/tests/connect/series/test_parity_cumulative.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesParityCumulativeTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_cummax`
Tests cumulative maximum ('cummax') logic within grouped DataFrames in pandas-on-Spark, ensuring correct results for varied grouping keys and multi-index scenarios.

#### `test_cummin`
Tests cumulative minimum ('cummin') logic within grouped DataFrames in pandas-on-Spark, covering various grouping scenarios and multi-index layouts.

#### `test_cumprod`
Tests cumulative product ('cumprod') logic within grouped DataFrames in pandas-on-Spark, with floating point awareness (non-exact comparisons) for varied group structures.

#### `test_cumsum`
Tests cumulative sum ('cumsum') logic within grouped DataFrames in pandas-on-Spark, validating correctness for multi-level groupings and index-heavy operations.

## File: `python/pyspark/pandas/tests/connect/series/test_parity_datetime.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesDateTimeParityTests`

#### `test_arithmetic_op_exceptions`
Verifies that invalid arithmetic operations on DatetimeIndexes (like addition, multiplication, division) correctly raise TypeErrors in pandas-on-Spark.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_ceil`
Tests the 'ceil' method on DatetimeIndexes in pandas-on-Spark for various frequencies, ensuring parity with pandas and verifying that nanosecond resolution is disallowed.

#### `test_date`
Validates the '.dt.date' accessor for datetime Series in pandas-on-Spark, ensuring it correctly extracts the date component.

#### `test_date_subtraction`
Tests subtraction operations between date components in pandas-on-Spark, ensuring the resulting difference is in days and verifying appropriate TypeErrors for invalid operand types.

#### `test_day`
Verifies that the `dt.day` accessor correctly extracts the day of the month from a Series of datetime values.

#### `test_day_name`
Verifies that the `day_name()` method on a DatetimeIndex correctly returns the name of the day for each datetime entry, comparing pandas-on-Spark results with pandas.

#### `test_dayofweek`
Tests the Spark SQL `dayofweek` function for extracting the day of the week from a datetime column, asserting consistency with a known date.

#### `test_dayofyear`
Verifies that the `dt.dayofyear` accessor correctly extracts the day of the year from a Series of datetime values.

#### `test_days_in_month`
Verifies that the `dt.days_in_month` accessor correctly returns the number of days in the month for a Series of datetime values.

#### `test_daysinmonth`
Verifies that the `dt.daysinmonth` accessor (alias for `days_in_month`) correctly returns the number of days in the month for a Series of datetime values.

#### `test_div`
Verifies that dividing the duration between two datetime columns by a time delta correctly calculates the numeric ratio for various time units (days, seconds, milliseconds).

#### `test_floor`
Verifies that the `floor()` method on a DatetimeIndex correctly rounds down to the nearest specified frequency, comparing pandas-on-Spark with pandas.

#### `test_hour`
Verifies that the Spark SQL `hour` function correctly extracts the hour from a column of time values, supporting both column and string name references.

#### `test_is_leap_year`
Verifies that the `dt.is_leap_year` accessor correctly identifies whether the year of each datetime entry in a Series is a leap year.

#### `test_is_month_end`
Verifies that the `dt.is_month_end` accessor correctly identifies whether each datetime entry in a Series is the last day of the month.

#### `test_is_month_start`
Verifies that the `dt.is_month_start` accessor correctly identifies whether each datetime entry in a Series is the first day of the month.

#### `test_is_quarter_end`
Verifies that the `dt.is_quarter_end` accessor correctly identifies whether each datetime entry in a Series is the last day of a quarter.

#### `test_is_quarter_start`
Verifies that the `dt.is_quarter_start` accessor correctly identifies whether each datetime entry in a Series is the first day of a quarter.

#### `test_is_year_end`
Verifies that the `dt.is_year_end` accessor correctly identifies whether each datetime entry in a Series is the last day of the year.

#### `test_is_year_start`
Verifies that the `dt.is_year_start` accessor correctly identifies whether each datetime entry in a Series is the first day of the year.

#### `test_isocalendar`
Verifies that the `isocalendar()` method on a DatetimeIndex correctly returns ISO year, week number, and weekday, ensuring consistency with pandas.

#### `test_microsecond`
Verifies that the `dt.microsecond` accessor correctly extracts the microseconds from a Series of datetime values.

#### `test_minute`
Verifies that the Spark SQL `minute` function correctly extracts the minutes from a column of time values, supporting both column and string name references.

#### `test_month`
Verifies that the `dt.month` accessor correctly extracts the month from a Series of datetime values.

#### `test_month_name`
This test verifies that the `month_name()` method for pandas-on-Spark datetime indexes returns the same results as the corresponding pandas index across multiple test cases.

#### `test_nanosecond`
This test ensures that accessing the `nanosecond` property via `.dt` on a series raises a `NotImplementedError`, indicating it is not currently supported.

#### `test_normalize`
This test verifies that the `normalize()` method for pandas-on-Spark datetime indexes, which resets time to midnight, maintains parity with the pandas implementation.

#### `test_quarter`
This test checks that the `quarter` property accessed via `.dt` on a series correctly returns the quarter of the year, matching pandas behavior.

#### `test_round`
This test verifies that the `round()` method on datetime indexes works correctly for various fixed frequencies and ensures that nanosecond-level rounding is explicitly disallowed.

#### `test_second`
This test verifies the SQL `second()` function correctly extracts the seconds component from a time object, specifically testing both column reference and string name access.

#### `test_strftime`
This test verifies that the `strftime()` method for datetime indexes correctly formats dates into strings using a specified pattern, matching pandas output.

#### `test_time`
This test ensures that accessing the `time` property via `.dt` on a series raises a `NotImplementedError`.

#### `test_timestamp_subtraction`
This test verifies the correctness of timestamp subtraction, including series-series subtraction and series-literal subtraction, ensuring the resulting duration matches pandas when converted to seconds.

#### `test_timestamp_subtraction_errors`
This test verifies that subtracting non-datetime types from a datetime series correctly raises a `TypeError` with an informative error message.

#### `test_timetz`
This test ensures that accessing the `timetz` property via `.dt` on a series raises a `NotImplementedError`.

#### `test_tz_convert`
This test verifies that the `tz_convert()` method correctly converts the timezone of a datetime series to the specified target timezone.

#### `test_tz_localize`
This test verifies that the `tz_localize()` method correctly localizes a timezone-naive datetime series to a specified timezone.

#### `test_unsupported_type`
This test verifies that attempting to use datetime-specific methods (via `.dt`) on a series with an incompatible data type (like LongType) raises an appropriate `ValueError`.

#### `test_weekday`
This test checks that the `weekday` property accessed via `.dt` correctly returns the day of the week for a datetime series.

#### `test_year`
This test checks that the `year` property accessed via `.dt` correctly extracts the year from each entry in a datetime series.

## File: `python/pyspark/pandas/tests/connect/series/test_parity_index.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesParityIndexTests`

#### `test_align`
This comprehensive test verifies the `align()` method for DataFrames and Series, checking various join types (outer, inner, left, right) and axes, and ensures that invalid inputs or unsupported alignments raise the expected exceptions.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_axes`
This test verifies that the `axes` property, which returns a list of row and column labels, matches pandas behavior for both standard and multi-indexed DataFrames.

#### `test_droplevel`
This test verifies the `droplevel()` method for DataFrames with multi-level indexes or columns, ensuring parity with pandas for dropping levels by name, position, or tuple, and checks for correct error handling when levels are missing or invalid.

#### `test_first_valid_index`
This test verifies that the `first_valid_index()` method correctly identifies the first non-null index in a DataFrame, handling various index types (standard, multi-index, datetime) and empty DataFrames.

#### `test_idxmax`
Verifies the idxmax function in a grouped pandas-on-Spark DataFrame and Series. It checks idxmax with and without skipna, on both single and multi-index columns, and ensures it raises a ValueError for multi-level indexes.

#### `test_idxmin`
Verifies the idxmin function in a grouped pandas-on-Spark DataFrame and Series. It checks idxmin with and without skipna, on both single and multi-index columns, and ensures it raises a ValueError for multi-level indexes.

#### `test_index`
Tests setting and getting names and multi-index names for pandas-on-Spark Series indexes, ensuring they match pandas behavior.

#### `test_index_to_series_reset_index`
Verifies the behavior of reset_index on a Series created from an index (to_series), including cases with different names and the drop parameter.

#### `test_last_valid_index`
Tests the last_valid_index method on pandas-on-Spark DataFrames, including cases with MultiIndex columns and empty DataFrames, ensuring parity with pandas.

#### `test_reindex`
Comprehensively tests the reindex method for pandas-on-Spark DataFrames and Indexes. It covers reindexing by columns and index, using fill_value, handling single and MultiIndex, and ensures proper error handling for invalid arguments.

#### `test_reindex_like`
Verifies the reindex_like method for pandas-on-Spark DataFrames, ensuring it correctly conforms one DataFrame's index and columns to another, including support for MultiIndex and proper error handling.

#### `test_rename_axis`
Tests the rename_axis method for pandas-on-Spark DataFrames, verifying it correctly renames index or column axes for both single and MultiIndex cases, including inplace operations and error handling for invalid inputs.

#### `test_reset_index`
Verifies the reset_index method for pandas-on-Spark DataFrames, checking basic functionality, drop=True, handling existing column names that conflict with the index name, and inplace operations.

#### `test_reset_index_with_default_index_types`
Tests reset_index on pandas-on-Spark DataFrames under different default index type configurations (sequence, distributed-sequence, distributed), ensuring consistent behavior where applicable.

#### `test_swapaxes`
Verifies the swapaxes method for pandas-on-Spark DataFrames, ensuring it correctly swaps index and column axes (for pandas versions < 3.0.0) and raises an AttributeError for newer versions.

#### `test_swaplevel`
Tests the swaplevel method for pandas-on-Spark DataFrames with MultiIndex on both rows and columns, verifying it correctly swaps specified levels and handles error conditions like invalid level indices or names.

## File: `python/pyspark/pandas/tests/connect/series/test_parity_interpolate.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesParityInterpolateTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_interpolate`
Verifies the interpolate method for pandas-on-Spark DataFrames using various datasets with missing values.

## File: `python/pyspark/pandas/tests/connect/series/test_parity_missing_data.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesParityMissingDataTests`

#### `test_add_and_radd_fill_value`
Tests the add and radd methods for pandas-on-Spark Series with the fill_value parameter, ensuring missing values are correctly handled during addition, and verifies that adding list-like objects with fill_value raises NotImplementedError.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_backfill`
Verifies the backfill method for pandas-on-Spark DataFrames, ensuring parity with pandas for backward filling of missing values, including inplace support (for pandas versions < 3.0.0).

#### `test_bfill`
Tests the bfill (backward fill) method on grouped pandas-on-Spark DataFrames and Series, including support for single and multi-index columns.

#### `test_dropna`
Tests the dropna method for PySpark DataFrames, verifying various parameters like how ('any', 'all'), subset, and thresh, and ensures correct error handling for invalid subset types.

#### `test_ffill`
Tests the ffill (forward fill) method on grouped pandas-on-Spark DataFrames and Series, including support for single and multi-index columns.

#### `test_fillna`
Verifies the fillna method for PySpark DataFrames, checking it correctly fills null values for different data types (int, double, bool, string), supports subset filtering, handles dictionary inputs, and provides proper error messages for invalid arguments.

#### `test_pad`
Verifies the pad method for pandas-on-Spark DataFrames, ensuring parity with pandas for forward filling of missing values, including inplace support (for pandas versions < 3.0.0).

#### `test_replace`
Verifies that the Spark Connect 'replace' and 'na.replace' operations correctly generate the underlying protobuf plan, ensuring that replacements for both numeric (double) and string values, as well as column subsets, are accurately captured in the plan.

## File: `python/pyspark/pandas/tests/connect/series/test_parity_series.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesParityTests`

#### `test_add_prefix`
Tests the 'add_prefix' method on pandas-on-Spark DataFrames, ensuring parity with pandas behavior for both standard and MultiIndex columns.

#### `test_add_suffix`
Tests the 'add_suffix' method on pandas-on-Spark DataFrames, ensuring parity with pandas behavior for both standard and MultiIndex columns.

#### `test_all_null_series`
Verifies that pandas-on-Spark correctly handles Series containing only null values for both numeric (float64) and string data types, with and without Arrow optimization enabled.

#### `test_and`
Tests the bitwise AND (&) operator for pandas-on-Spark Series containing boolean and null values, ensuring parity with pandas when combined with boolean literals and None.

#### `test_and_extenstion_dtypes`
Tests the bitwise AND (&) operator for pandas-on-Spark Series using pandas' extension 'boolean' dtype, verifying correct behavior with boolean literals and pd.NA.

#### `test_apply`
Verifies the 'apply' method on grouped pandas-on-Spark DataFrames, ensuring parity with pandas behavior and correctly handling the 'include_groups' parameter across different pandas versions (including the removal of include_groups=True in pandas 3.0+).

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_combine_first`
Tests the 'combine_first' method for pandas-on-Spark Series, ensuring that null values in one Series are correctly filled by values from another Series, matching pandas behavior.

#### `test_creation_index`
Extensively tests the creation of pandas-on-Spark DataFrames using various index types (standard, String, Datetime, and MultiIndex) from both local and distributed data sources, verifying parity with pandas and correct behavior for cross-DataFrame operations configuration.

#### `test_dot`
Verifies the dot product operation for Spark ML vectors (SparseVector and DenseVector) against other vectors, matrices, and arrays.

#### `test_empty_series`
Tests the creation and basic properties of empty pandas-on-Spark Series for different data types, ensuring parity with pandas both with and without Arrow enabled.

#### `test_eq`
Tests semantic equality between various Spark ML vector and matrix types, ensuring that dense and sparse representations are considered equal if they contain the same values.

#### `test_filter`
Verifies that the Spark Connect 'filter' operation correctly generates a protobuf plan with the expected unresolved comparison function and its arguments.

#### `test_first`
Tests the 'first()' API in the Spark Connect Python client, verifying that it returns the first row for populated DataFrames and None for empty ones.

#### `test_getitem`
Verifies that selecting a column from a grouped pandas-on-Spark DataFrame using __getitem__ correctly returns a SeriesGroupBy object.

#### `test_head`
Tests the 'head()' API in the Spark Connect Python client, verifying its behavior for different row counts and ensuring it returns None for empty DataFrames.

#### `test_isin`
Tests the 'isin' column operation in Spark Connect, ensuring parity with standard PySpark when using literals, lists, sets, other columns, or a mix of columns and literals.

#### `test_item`
Verifies the 'item()' method for pandas-on-Spark Index and MultiIndex objects, ensuring it correctly extracts a single scalar value and raises a ValueError if the index does not contain exactly one element.

#### `test_items`
Tests the 'items()' method for pandas-on-Spark DataFrames, ensuring that it iterates over column names and Series in the same way as pandas.

#### `test_keys`
Verifies that the 'keys()' method for pandas-on-Spark DataFrames returns the correct column labels, matching pandas behavior.

#### `test_last`
Tests the last() statistical function on grouped series/dataframes, including cases with numeric_only and min_count parameters, comparing pandas-on-Spark results with pandas.

#### `test_map`
Tests the map() function on pandas-on-Spark Indices using dictionaries, lambdas, and Series, ensuring parity with pandas.

#### `test_mask`
Tests that mask() on a pandas-on-Spark DataFrame raises a TypeError when the condition is not a DataFrame or Series.

#### `test_missing`
Tests that calling unimplemented pandas-like general functions in the pyspark.pandas namespace raises a PandasNotImplementedError.

#### `test_notnull`
Tests the notnull() method on pandas-on-Spark Series, ensuring parity with pandas.

#### `test_or`
Tests the bitwise OR (|) operator on pandas-on-Spark Series with boolean values and None/np.nan, ensuring parity with pandas.

#### `test_or_extenstion_dtypes`
Tests the bitwise OR (|) operator on pandas-on-Spark Series using nullable boolean extension dtypes (boolean), ensuring parity with pandas.

#### `test_rename`
Tests renaming pandas-on-Spark Series and Indices by setting the .name attribute, and verifies that invalid (non-hashable) names raise a TypeError.

#### `test_rename_method`
Tests the rename() method on pandas-on-Spark Series using string names and lambdas, including inplace=True support and error handling for unsupported inputs.

#### `test_repeat`
A placeholder test that currently does nothing (passes).

#### `test_repr_cache_invalidation`
Verifies that in-place operations on pandas-on-Spark DataFrames correctly invalidate the cached __repr__ string.

#### `test_series_from_series`
Tests creating a pandas-on-Spark Series from another Series, including specifying a new index, dtype, or name.

#### `test_series_ops`
Tests various arithmetic operations (like addition) between pandas-on-Spark Series and Indices, including cross-object operations and broadcasting error handling.

#### `test_series_tuple_name`
Tests that pandas-on-Spark Series can correctly handle tuple names, ensuring parity with pandas.

#### `test_shape`
Tests the shape property of pandas-on-Spark Series, including those with MultiIndex.

#### `test_squeeze`
Tests the squeeze() method on pandas-on-Spark DataFrames with various axes and single/multiple column/value combinations, ensuring parity with pandas.

#### `test_tail`
Tests the tail() method in Spark Connect by comparing results between the Connect client and the regular Spark session.

#### `test_take`
Tests the take() API in the Spark Connect Python client, verifying it returns the correct number of rows for both populated and empty DataFrames.

#### `test_transform`
Tests the transform() method on Spark Connect columns using both built-in functions (like trim, upper) and lambda expressions, ensuring parity with regular Spark.

#### `test_udt`
A parity test for User-Defined Types (UDT), calling the superclass implementation.

#### `test_update`
Tests the `update` method for pandas-on-Spark DataFrames, comparing its behavior with pandas for in-place updates, handling of the `overwrite` parameter, and support for MultiIndex columns.

#### `test_where`
Verifies that calling `where` on a Spark SQL DataFrame with an invalid condition type (int) raises a `PySparkTypeError` with the correct error class and message parameters.

#### `test_xs`
Tests the `xs` (cross-section) method for pandas-on-Spark DataFrames, verifying data selection by index labels, levels, and MultiIndex handling, while ensuring appropriate errors for unsupported axes or invalid keys.

## File: `python/pyspark/pandas/tests/connect/series/test_parity_sort.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesParitySortTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_searchsorted`
Tests the `searchsorted` method for pandas-on-Spark Series across different data types (numeric, datetime, MultiIndex), verifying the insertion index for various values and 'side' parameters against pandas behavior.

#### `test_sort_index`
Tests the `sort_index` method for pandas-on-Spark DataFrames, verifying various sorting options such as ascending/descending, NA position, in-place sorting, and MultiIndex support.

#### `test_sort_values`
Tests the `sort_values` method for pandas-on-Spark Index and MultiIndex objects, ensuring correct ordering and handling of index names.

## File: `python/pyspark/pandas/tests/connect/series/test_parity_stat.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesParityStatTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_autocorr`
Tests the `autocorr` (autocorrelation) method for pandas-on-Spark Series, verifying calculation correctness with various datasets (including NaNs) and ensuring it rejects non-integer lag parameters.

#### `test_cov`
Tests the `cov` (sample covariance) method for Spark SQL DataFrames, checking calculation accuracy and verifying that passing non-string column names raises the expected `PySparkTypeError`.

#### `test_div_zero_and_nan`
Tests division operations (`div`, `truediv`, `floordiv`) by zero and NaN for pandas-on-Spark Series, ensuring parity with pandas behavior for various edge cases including infinite values.

#### `test_divmod`
Tests the `divmod` method for pandas-on-Spark Series, verifying that both the quotient and remainder match pandas results when dividing by integers.

#### `test_hasnans`
Tests the `hasnans` property for pandas-on-Spark Index and Series objects across Boolean and Timestamp types, verifying correct identification of missing values and ensuring it raises `NotImplementedError` for MultiIndex.

#### `test_is_unique`
Tests the `is_unique` property for pandas-on-Spark Series, verifying correct uniqueness checks for single-value, duplicated, and null-containing Series.

#### `test_median`
Verifies that the `median` function in Spark SQL correctly renders its string representation as 'median(col_name)' when applied.

#### `test_mod`
Tests the modulo (`mod`) operator for pandas-on-Spark Series, ensuring element-wise parity with pandas when operating between two Series.

#### `test_mode`
Tests the `mode` method for pandas-on-Spark DataFrames, verifying its ability to identify the most frequent values while supporting `numeric_only` and `dropna` parameters, and checking its integration with `mapInPandas`.

#### `test_nlargest`
Tests the `nlargest` method within a groupby operation for pandas-on-Spark DataFrames, ensuring it correctly retrieves the top 'n' values for each group across various column and index naming scenarios.

#### `test_nsmallest`
Tests the `nsmallest` method within a groupby operation for pandas-on-Spark DataFrames, ensuring it correctly retrieves the smallest 'n' values for each group across various column and index naming scenarios.

#### `test_nunique`
Tests the `nunique` aggregation within a groupby operation for pandas-on-Spark DataFrames, verifying results for single and MultiIndex columns, and checking the `dropna` parameter's effect.

#### `test_pct_change`
Tests the `pct_change` (percentage change) method for pandas-on-Spark DataFrames, verifying its calculations for specified periods and its handling of MultiIndex columns.

#### `test_pow_and_rpow`
Tests the power (`pow`, `**`) and reverse power (`rpow`) operations for pandas-on-Spark Series, ensuring parity with pandas for element-wise exponentiation (including NaN handling).

#### `test_product`
Verifies the `product` method for both Series and DataFrames, including parameters like `axis` and `min_count`, and handling of empty selections.

#### `test_quantile`
Tests the `quantile` method for grouped DataFrames and Series, checking various quantile values, interpolation, and error handling for invalid quantiles or data types.

#### `test_rank`
Tests the `rank` method on grouped DataFrames and Series, including multi-index columns and different grouping keys.

#### `test_rdivmod`
Tests the `rdivmod` operator on a Series with null values against scalar values.

#### `test_rmod`
Tests the `rmod` operator between two Series, specifically checking compatibility when operating across different frames.

#### `test_round`
Tests the `round` method on DatetimeIndex objects with various frequencies and ensures that nanosecond rounding is disallowed.

#### `test_series_stat_fail`
Verifies that statistical methods like `mean`, `skew`, `std`, etc., correctly raise a `TypeError` when called on non-numeric Series.

#### `test_value_counts`
Tests the `value_counts` method on grouped Series, including parameters like `dropna`, `sort`, `ascending`, and handling of renamed Series/grouping keys.

## File: `python/pyspark/pandas/tests/connect/series/test_parity_string_ops_adv.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesParityStringOpsAdvTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_string_decode`
Ensures that calling `str.decode` on a Series raises a `NotImplementedError`.

#### `test_string_encode`
Ensures that calling `str.encode` on a Series raises a `NotImplementedError`.

#### `test_string_extract`
Ensures that calling `str.extract` on a Series raises a `NotImplementedError`.

#### `test_string_extractall`
Ensures that calling `str.extractall` on a Series raises a `NotImplementedError`.

#### `test_string_find`
Tests the `str.find` method on a Series, checking substrings with optional `start` and `end` indices.

#### `test_string_findall`
Tests the `str.findall` method with regular expressions and flags, accounting for pandas version differences in output types.

#### `test_string_get_dummies`
Ensures that calling `str.get_dummies` on a Series raises a `NotImplementedError`.

#### `test_string_index`
Tests the `str.index` method, verifying it works for finding substrings and correctly raises exceptions for missing patterns or invalid ranges.

#### `test_string_join`
Tests the `str.join` method for joining elements within list-like entries of a Series using a separator.

#### `test_string_len`
Tests the `str.len` method for both string Series and Series containing list-like objects.

#### `test_string_ljust`
Tests the `str.ljust` method for left-aligning strings with a specified width and optional fill character.

#### `test_string_match`
Tests the `str.match` method with various regex patterns, case sensitivity settings, and flags.

#### `test_string_normalize`
Tests Unicode normalization (NFC and NFKD forms) for string Series.

#### `test_string_pad`
Tests string padding with various sides (left, both, right) and custom fill characters.

#### `test_string_partition`
Verifies that the string partition method is not yet implemented and raises the appropriate error.

#### `test_string_repeat`
Tests string repetition with a constant factor and verifies that passing a list of repeats raises a TypeError.

#### `test_string_replace`
Tests complex string replacement using regex literals, flags, and callable functions for replacement logic.

#### `test_string_rfind`
Tests searching for the highest index of a substring within a specified range in a string Series.

#### `test_string_rindex`
Tests finding the highest index of a substring, ensuring exceptions are raised when the substring is not found or out of range.

#### `test_string_rjust`
Tests right-justifying strings in a Series to a specific width with optional padding characters.

#### `test_string_rpartition`
Verifies that the string rpartition method is not yet implemented and raises the appropriate error.

#### `test_string_rsplit`
Tests splitting strings from the right with various delimiters, split limits, and output expansion options.

#### `test_string_slice`
Tests slicing strings in a Series using start, stop, and step parameters.

#### `test_string_slice_replace`
Tests replacing a specific slice of strings in a Series with a replacement string.

#### `test_string_split`
Tests splitting strings from the left with various delimiters, split limits, and output expansion options.

#### `test_string_translate`
Tests character-level translation of strings in a Series using a translation table mapping.

#### `test_string_wrap`
Tests wrapping long strings in a Series into multiple lines with various configuration options for whitespace and word breaking.

#### `test_string_zfill`
Tests zero-filling strings in a Series to a specified minimum width.

## File: `python/pyspark/pandas/tests/connect/series/test_parity_string_ops_basic.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SeriesStringOpsParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_string_add_assign`
Tests the in-place addition and assignment (+=) of two string columns in a DataFrame.

#### `test_string_add_str_lit`
Tests concatenating a string literal to the beginning or end of a string Series.

#### `test_string_add_str_num`
Verifies that attempting to add a numeric Series to a string Series raises a TypeError.

#### `test_string_add_str_str`
Tests the element-wise concatenation of two string Series.

#### `test_string_capitalize`
Tests the `str.capitalize()` method on a Series, which converts the first character of each string to uppercase and the rest to lowercase.

#### `test_string_cat`
Verifies that the `str.cat()` method is not yet implemented for Spark Connect Series by asserting that it raises a `NotImplementedError`.

#### `test_string_center`
Tests the `str.center()` method with different widths and fill characters to ensure strings are correctly centered within a specified width.

#### `test_string_contains`
Tests the `str.contains()` method for substring search using both literal strings and regular expressions, with support for case sensitivity and handling of missing values.

#### `test_string_count`
Tests the `str.count()` method to count occurrences of a pattern (regular expression) in each string, including case-insensitive matching.

#### `test_string_endswith`
Tests the `str.endswith()` method to check if each string ends with a specified suffix, including handling of missing values.

#### `test_string_get`
Tests the `str.get()` method to extract a character at a specific position (index) from each string, including negative indexing.

#### `test_string_isalnum`
Tests the `str.isalnum()` method to check if all characters in each string are alphanumeric.

#### `test_string_isalpha`
Tests the `str.isalpha()` method to check if all characters in each string are alphabetic.

#### `test_string_isdecimal`
Tests the `str.isdecimal()` method to check if all characters in each string are decimals.

#### `test_string_isdigit`
Tests the `str.isdigit()` method to check if all characters in each string are digits.

#### `test_string_islower`
Tests the `str.islower()` method to check if all cased characters in each string are lowercase.

#### `test_string_isnumeric`
Tests the `str.isnumeric()` method to check if all characters in each string are numeric.

#### `test_string_isspace`
Tests the `str.isspace()` method to check if all characters in each string are whitespace.

#### `test_string_istitle`
Tests the `str.istitle()` method to check if each string is titlecased (first character of each word is uppercase, others lowercase).

#### `test_string_isupper`
Tests the `str.isupper()` method to check if all cased characters in each string are uppercase.

#### `test_string_lower`
Tests the `str.lower()` method to convert all characters in each string to lowercase.

#### `test_string_lstrip`
Tests the `str.lstrip()` method to remove leading characters (whitespace or specified character set) from each string.

#### `test_string_rstrip`
Tests the `str.rstrip()` method to remove trailing characters (whitespace or specified character set) from each string.

#### `test_string_startswith`
Tests the `str.startswith()` method to check if each string starts with a specified prefix, including handling of missing values.

#### `test_string_strip`
Verifies Series.str.strip() functionality with and without arguments (stripping default whitespace, specific characters, and numbers).

#### `test_string_swapcase`
Verifies Series.str.swapcase() functionality, which swaps uppercase characters to lowercase and vice versa.

#### `test_string_title`
Verifies Series.str.title() functionality, which converts the first character of each word to uppercase and the rest to lowercase.

#### `test_string_upper`
Verifies Series.str.upper() functionality, which converts all characters to uppercase.

## File: `python/pyspark/pandas/tests/connect/test_parity_arrow_interface.py`

### Class: `ArrowInterfaceParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_spark_arrow_c_streamer_arrow_consumer`
Tests the Arrow C stream interface by converting a pandas-on-Spark DataFrame to an Arrow stream capsule and then back to a pyarrow Table, verifying the data integrity and schema.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/test_parity_categorical.py`

### Class: `CategoricalParityTests`

#### `test_add_categories`
Verifies Series.cat.add_categories() functionality, including adding single and multiple categories, adding an empty list, and handling duplicate categories (which should raise a ValueError).

#### `test_as_ordered_unordered`
Verifies Series.cat.as_ordered() and Series.cat.as_unordered() functionality for categorical data.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_astype`
Verifies astype("category") and astype(CategoricalDtype(...)) for Series, ensuring parity between pandas and pandas-on-Spark for categorical type casting.

#### `test_categorical_frame`
Verifies basic operations on a DataFrame with categorical columns, including accessors, sorting by index, and sorting by values.

#### `test_categorical_series`
Verifies basic operations on a categorical Series, including checking categories, codes, and the ordered flag, and ensuring an error is raised when accessing .cat on a non-categorical Series.

#### `test_categories_setter`
Verifies renaming categories using Series.cat.rename_categories() and ensures setting the categories property directly with an incorrect number of categories raises a ValueError.

#### `test_factorize`
Verifies the factorize() method on categorical Series, including handling of null values and custom na_sentinel.

#### `test_frame_apply`
Verifies DataFrame.apply() with the identity function on categorical data, across both rows and columns.

#### `test_frame_apply_batch`
Verifies DataFrame.pandas_on_spark.apply_batch() functionality for categorical data, including type casting to string and specific categorical dtypes.

#### `test_frame_apply_batch_without_shortcut`
Similar to test_frame_apply_batch but tests the functionality when compute.shortcut_limit is set to 0, ensuring correct behavior in distributed execution.

#### `test_frame_apply_without_shortcut`
Similar to test_frame_apply but tests the functionality when compute.shortcut_limit is set to 0, ensuring correct behavior in distributed execution.

#### `test_frame_transform`
Verifies DataFrame.transform() for categorical data, including using cat.codes and astype(dtype) transformations.

#### `test_frame_transform_batch`
Verifies DataFrame.pandas_on_spark.transform_batch() for categorical data, including casting to string and categorical codes/dtypes.

#### `test_frame_transform_batch_without_shortcut`
Similar to test_frame_transform_batch but tests the functionality when compute.shortcut_limit is set to 0, ensuring correct behavior in distributed execution.

#### `test_frame_transform_without_shortcut`
Similar to test_frame_transform but tests the functionality when compute.shortcut_limit is set to 0, ensuring correct behavior in distributed execution.

#### `test_groupby_apply`
Verifies that `groupby.apply` works correctly for categorical data, including applying functions that return the same dataframe, specific columns, or categorical codes.

#### `test_groupby_apply_without_shortcut`
Tests `groupby.apply` for categorical data with `compute.shortcut_limit` set to 0, ensuring correct behavior when shortcuts are disabled.

#### `test_groupby_transform`
Checks `groupby.transform` for categorical series, including identity transformations and type casting within the transform.

#### `test_groupby_transform_without_shortcut`
Tests `groupby.transform` for categorical data with `compute.shortcut_limit` set to 0, validating behavior when internal optimizations are bypassed.

#### `test_remove_categories`
Verifies the `cat.remove_categories` method for categorical series, testing with various input types (single value, list, empty list, None) and ensuring correct error handling for non-existent categories.

#### `test_remove_unused_categories`
Validates `cat.remove_unused_categories`, ensuring it correctly identifies and removes categories that are not present in the data after additions or removals.

#### `test_rename_categories`
Checks the `cat.rename_categories` method, testing renaming with lists, dictionaries, and mapping functions, and verifying error cases for size mismatches or invalid types.

#### `test_reorder_categories`
Verifies `cat.reorder_categories`, testing reordering with and without setting the `ordered` flag, and ensuring proper validation of input categories.

#### `test_series_apply`
Tests the `apply` method on a categorical series to ensure it correctly preserves or handles categorical data during row-wise application of a function.

#### `test_series_apply_without_shortcut`
Tests `series.apply` for categorical data with `compute.shortcut_limit` set to 0, ensuring parity when shortcuts are disabled.

#### `test_series_transform_batch`
Verifies `transform_batch` (a pandas-on-spark specific method) for categorical series, testing transformations that convert to strings or different categorical types.

#### `test_series_transform_batch_without_shortcut`
Tests `series.transform_batch` for categorical data with `compute.shortcut_limit` set to 0, validating parity when optimizations are disabled.

#### `test_set_categories`
Checks the `cat.set_categories` method, testing various scenarios including adding, removing, and renaming categories, as well as setting the `ordered` property.

#### `test_unstack`
Verifies that `unstack` works correctly on categorical series with multi-level indices, ensuring the resulting dataframes match expectations.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/test_parity_config.py`

### Class: `ConfigParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_check_func`
Validates that configuration options with validation functions correctly raise `ValueError` when set to invalid values (e.g., negative integers).

#### `test_different_types`
Ensures that `set_option` raises `TypeError` when attempting to set configuration values to types that do not match the expected type(s).

#### `test_dir_options`
Checks that the `dir()` function on `ps.options` and its sub-namespaces correctly lists the available configuration options.

#### `test_get_set_reset_option`
Verifies the basic lifecycle of a configuration option: getting the default value, setting a new value, and resetting it back to the default.

#### `test_get_set_reset_option_different_types`
Tests getting, setting, and resetting configuration options of various types (list, float, int, and int with None allowed).

#### `test_namespace_access`
Validates accessing and modifying configuration options through the `ps.options` object hierarchy, and ensures proper error handling for non-existent options.

#### `test_unknown_option`
Verifies that ps.get_option, ps.set_option, and ps.reset_option raise config.OptionError when called with an unknown option name.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/test_parity_extension.py`

### Class: `ExtensionParityTests`

#### `test_accessor_works`
Ensures that a custom series accessor can be registered and that its properties and methods work correctly on a pandas-on-Spark Series.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_dataframe_register`
Validates that a custom dataframe accessor can be registered and that its members can be accessed from a pandas-on-Spark DataFrame.

#### `test_index_register`
Confirms that a custom index accessor can be registered and used to access properties and methods from a pandas-on-Spark Index.

#### `test_overwrite_warns`
Checks that registering a series accessor with a name that already exists (like 'mean') produces a UserWarning.

#### `test_raises_attr_error`
Tests that an AttributeError is correctly raised when accessing an accessor that fails during initialization.

#### `test_series_register`
Verifies that a custom series accessor can be registered and its members accessed from a pandas-on-Spark Series.

#### `test_setup`
A simple setup test to ensure the test accessor's item property is initialized correctly.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/test_parity_frame_spark.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SparkFrameMethodsParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_checkpoint`
Inherited test that presumably verifies the checkpoint method on a pandas-on-Spark DataFrame.

#### `test_coalesce`
Inherited test that presumably verifies the coalesce method on a pandas-on-Spark DataFrame.

#### `test_frame_apply_negative`
Ensures that ps.DataFrame.spark.apply raises a ValueError if the function passed to it does not return a pyspark.sql.DataFrame.

#### `test_hint`
Comprehensive test for the hint method on a Spark Connect DataFrame, verifying both supported and unsupported hints, parameters, and types.

#### `test_local_checkpoint`
Verifies that ps.DataFrame.spark.local_checkpoint returns a DataFrame equal to the original one.

#### `test_repartition`
Inherited test that presumably verifies the repartition method on a pandas-on-Spark DataFrame.

## File: `python/pyspark/pandas/tests/connect/test_parity_generic_functions.py`

### Class: `GenericFunctionsParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_prod_precision`
Tests the product function (prod) on a pandas-on-Spark DataFrame with various parameters like skipna and min_count, ensuring precision parity with pandas.

#### `test_stat_functions`
Broad test suite for various statistical functions (sum, mean, product, min, max, std, var, sem, skew, median, kurtosis) on pandas-on-Spark DataFrames and Series, including error handling for invalid parameters.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/test_parity_indexops_spark.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SparkIndexOpsMethodsParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_multiindex_transform_negative`
Confirms that spark.transform on a MultiIndex raises a NotImplementedError.

#### `test_series_apply_negative`
Validates error handling for ps.Series.spark.apply and ps.Series.spark.transform when given invalid functions or non-existent columns.

#### `test_series_transform_negative`
Similar to the apply negative test, it verifies that ps.Series.spark.transform handles invalid return types and unresolved columns correctly.

## File: `python/pyspark/pandas/tests/connect/test_parity_internal.py`

### Class: `InternalFrameParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_attach_distributed_column`
Tests the InternalFrame.attach_distributed_sequence_column method for adding a distributed sequence column to Spark DataFrames with varying numbers of existing columns, including empty ones.

#### `test_from_pandas`
Verifies that ps.from_pandas correctly converts various Pandas objects (DataFrame, Series, Index, and MultiIndex) into their Spark-on-Pandas equivalents, and ensures that passing an unsupported data type (like a Spark-on-Pandas Index) to ps.from_pandas raises a TypeError.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/test_parity_namespace.py`

### Class: `NamespaceParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_broadcast`
Verifies that the `broadcast` function works correctly when joining DataFrames, and that it raises a `TypeError` if passed a Column instead of a DataFrame.

#### `test_concat_column_axis`
Tests the concatenation of pandas-on-Spark DataFrames and Series along the column axis (axis=1), comparing with standard pandas behavior under various `ignore_index` and `join` conditions.

#### `test_concat_index_axis`
Tests the concatenation of pandas-on-Spark DataFrames and Series along the index axis (axis=0), comparing with pandas behavior for both single and multi-indexed DataFrames, and verifying expected error cases.

#### `test_concat_multiindex_sort`
Verifies that `ps.concat` respects the `sort` parameter when concatenating multi-indexed DataFrames, matching pandas behavior.

#### `test_date_range`
Tests the `date_range` function with various parameters such as `start`, `end`, `periods`, `freq`, and `inclusive`, comparing the results against pandas' `date_range`.

#### `test_from_pandas`
Verifies that `ps.from_pandas` correctly converts pandas DataFrames, Series, and Indexes into their pandas-on-Spark counterparts, and raises a `TypeError` for unsupported input types.

#### `test_get_index_map`
Tests the internal `_get_index_map` function to ensure it correctly retrieves the index mapping for a given Spark DataFrame and specified index columns.

#### `test_json_normalize`
Tests `ps.json_normalize` with various JSON structures (simple, nested, with lists, different data types, empty input) and verifies that the output matches pandas' `json_normalize`.

#### `test_missing`
Verifies that unsupported functions in `MissingPandasLikeGeneralFunctions` raise a `PandasNotImplementedError` when called.

#### `test_read_delta_with_wrong_input`
Verifies that `read_delta` raises a `ValueError` when both `version` and `timestamp` are specified.

#### `test_timedelta_range`
Tests `ps.timedelta_range` with various combinations of `start`, `end`, `periods`, and `freq`, comparing with pandas behavior.

#### `test_to_datetime`
Tests `ps.to_datetime` with various inputs like DataFrames, dicts, integers, and lists with different units and origins, comparing against pandas behavior.

#### `test_to_numeric`
Tests `ps.to_numeric` with both Series and list-like data, checking error handling modes ("coerce", "raise", "ignore") and comparing with pandas.

#### `test_to_timedelta`
Tests `ps.to_timedelta` with string, list, and Series inputs, comparing the results with pandas' `to_timedelta`.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/test_parity_numpy_compat.py`

### Class: `NumPyCompatParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_np_add_index`
Verifies that `np.add` works correctly when applied to pandas-on-Spark Indexes, comparing with pandas behavior.

#### `test_np_add_series`
Verifies that `np.add` works correctly when applied to pandas-on-Spark Series (adding two Series or a Series and a scalar), comparing with pandas behavior.

#### `test_np_spark_compat_frame`
Tests various NumPy ufuncs on pandas-on-Spark DataFrames, checking both unary and binary operations, and verifying compatibility with random data.

#### `test_np_spark_compat_series`
Similar to above, but tests NumPy ufuncs on pandas-on-Spark Series.

#### `test_np_unsupported_frame`
Verifies that calling unsupported NumPy functions on DataFrames raises appropriate errors (NotImplementedError or ValueError).

#### `test_np_unsupported_series`
Verifies that calling unsupported NumPy functions on Series raises a `NotImplementedError`.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/test_parity_repr.py`

### Class: `ReprParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_html_repr`
Tests the HTML representation (`_repr_html_`) of pandas-on-Spark DataFrames, verifying that it handles truncation correctly when exceeding `display.max_rows` and matches pandas behavior.

#### `test_repr_dataframe`
Tests the string representation (`__repr__`) of pandas-on-Spark DataFrames, ensuring that it truncates large DataFrames appropriately and includes the truncation notice message, matching pandas.

#### `test_repr_float_index`
Verifies that DataFrames, Series, and Indexes with float data types produce consistent string and HTML representations without unnecessary truncation messages when within the display limit.

#### `test_repr_indexes`
Tests the representation of various index types (single and multi-index) under different display limits, comparing with pandas representations.

#### `test_repr_series`
Tests the string representation of pandas-on-Spark Series (named and unnamed) and Series with multi-indexes, verifying truncation and comparison with pandas.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/test_parity_scalars.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `ScalarParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_missing`
Verifies that calling unsupported general functions raises a `PandasNotImplementedError`. (Duplicate of test at line 36)

## File: `python/pyspark/pandas/tests/connect/test_parity_spark_functions.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SparkFunctionsParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_repeat`
Placeholder test for the `repeat` function, currently empty.

## File: `python/pyspark/pandas/tests/connect/test_parity_sql.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SQLParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_error_bad_sql`
Verifies that passing invalid SQL syntax to `ps.sql` raises a `ParseException`.

#### `test_error_variable_not_exist`
Verifies that referencing a non-existent variable in an SQL query raises a `KeyError`.

#### `test_series_not_referred`
Verifies that a `ValueError` is raised if a Series argument is provided to `ps.sql` but not referenced in the SQL string.

#### `test_sql_with_index_col`
Tests the `index_col` parameter in `ps.sql` with both single and multi-indexed DataFrames, verifying that the result DataFrame has the correct index.

#### `test_sql_with_pandas_objects`
Tests that `ps.sql` can handle standard pandas DataFrames and Series passed as variables, returning the expected results.

#### `test_sql_with_pandas_on_spark_objects`
Tests that `ps.sql` can reference columns from pandas-on-Spark DataFrames passed in as variables.

#### `test_sql_with_python_objects`
Verifies that python literals and tuples passed to `ps.sql` as arguments are parsed correctly.

## File: `python/pyspark/pandas/tests/connect/test_parity_typedef.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `TypeHintParityTests`

#### `test_as_spark_type_extension_dtypes`
Verifies that pandas extension integer dtypes map correctly to their corresponding Spark `ByteType`, `ShortType`, `IntegerType`, and `LongType`.

#### `test_as_spark_type_extension_float_dtypes`
Verifies that pandas extension float dtypes map correctly to Spark `FloatType` and `DoubleType`.

#### `test_as_spark_type_extension_object_dtypes`
Verifies that pandas extension boolean and string dtypes map to Spark `BooleanType` and `StringType`.

#### `test_as_spark_type_pandas_on_spark_dtype`
Comprehensive test mapping diverse NumPy and Python types to their inferred Spark data types, and checking that unsupported types raise a `TypeError`.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_if_pandas_implements_class_getitem`
Verifies that pandas DataFrames and Series do not implement the `__class_getitem__` method, which is an assumption in the type hinting code.

#### `test_infer_schema_from_pandas_instances`
Verifies that schemas can be correctly inferred from specified types in standard function definitions.

#### `test_infer_schema_with_names_negative`
Verifies that schema inference throws a `TypeError` for unsupported type formats or object types lacking a clear mapping.

#### `test_infer_schema_with_names_pandas_instances`
Verifies that column names and data types are parsed correctly from functions returning types specified with column names in strings.

#### `test_infer_schema_with_names_pandas_instances_negative`
Similar to test at line 164 but applies specifically to scenarios where inference fails on pandas instances.

#### `test_infer_schema_with_no_return`
Verifies that schema inference raises errors when return types are omitted or specified as `None`.

## File: `python/pyspark/pandas/tests/connect/test_parity_utils.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `UtilsParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_dataframe_error_assert_pandas_almost_equal`
Verifies that `_assert_pandas_almost_equal` raises a `PySparkAssertionError` with error class "DIFFERENT_PANDAS_DATAFRAME" when comparing two different DataFrames.

#### `test_dataframe_error_assert_pandas_equal`
Verifies that `_assert_pandas_equal` raises a `PySparkAssertionError` with error class "DIFFERENT_PANDAS_DATAFRAME" when comparing two non-equal DataFrames.

#### `test_index_error_assert_pandas_almost_equal`
Verifies that `_assert_pandas_almost_equal` raises a `PySparkAssertionError` with error class "DIFFERENT_PANDAS_INDEX" when comparing two different Indexes.

#### `test_index_error_assert_pandas_equal`
Verifies that `_assert_pandas_equal` raises a `PySparkAssertionError` with error class "DIFFERENT_PANDAS_INDEX" when comparing two non-equal Indexes.

#### `test_lazy_property`
Verifies that lazy properties are cached after the first access.

#### `test_multiindex_error_assert_pandas_almost_equal`
Verifies that `_assert_pandas_almost_equal` raises a `PySparkAssertionError` with error class "DIFFERENT_PANDAS_MULTIINDEX" when comparing two different MultiIndexes.

#### `test_series_error_assert_pandas_almost_equal_2`
Verifies that `_assert_pandas_almost_equal` raises a `PySparkAssertionError` with error class "DIFFERENT_PANDAS_SERIES" when comparing two different Series.

#### `test_series_error_assert_pandas_equal`
Verifies that `_assert_pandas_equal` raises a `PySparkAssertionError` with error class "DIFFERENT_PANDAS_SERIES" when comparing two non-equal Series.

#### `test_validate_arguments_and_invoke_function`
Tests argument validation for functions, ensuring that unsupported parameters raise a `TypeError` if set to non-default values.

#### `test_validate_bool_kwarg`
Verifies that boolean argument validation correctly checks for boolean values and raises a `TypeError` otherwise.

#### `test_validate_index_loc`
Tests that out-of-bounds index lookups raise an `IndexError`.

#### `test_validate_mode`
Verifies that file write modes like 'w' and 'a' map to their appropriate full strings and throws an error for unsupported strings.

## File: `python/pyspark/pandas/tests/connect/window/test_parity_ewm_error.py`

### Class: `EWMParityErrorTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_ewm_error`
Comprehensive test verifying that various invalid parameters passed to exponentially weighted moving operations (`ewm`) raise appropriate `ValueError` or `TypeError` exceptions.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/window/test_parity_ewm_mean.py`

### Class: `EWMParityMeanTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_ewm_mean`
Dispatches a test specific to checking moving averages calculated via exponentially weighted windows.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/window/test_parity_expanding.py`

### Class: `ExpandingParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_expanding_count`
Tests expanding windows with the `count` aggregation.

#### `test_expanding_max`
Tests expanding windows with the `max` aggregation.

#### `test_expanding_mean`
Tests expanding windows with the `mean` aggregation.

#### `test_expanding_min`
Tests expanding windows with the `min` aggregation.

#### `test_expanding_repr`
Verifies the string representation of an expanding window object.

#### `test_expanding_sum`
Tests expanding windows with the `sum` aggregation.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/window/test_parity_expanding_adv.py`

### Class: `ExpandingAdvParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_expanding_kurt`
Tests expanding windows with the `kurt` (kurtosis) aggregation.

#### `test_expanding_quantile`
Tests expanding windows with the `quantile` aggregation.

#### `test_expanding_skew`
Tests expanding windows with the `skew` aggregation.

#### `test_expanding_std`
Tests expanding windows with the `std` (standard deviation) aggregation.

#### `test_expanding_var`
Tests expanding windows with the `var` (variance) aggregation.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/window/test_parity_expanding_error.py`

### Class: `ExpandingErrorParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_expanding_error`
Verifies that invalid inputs to expanding window operations raise expected errors.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/window/test_parity_groupby_ewm_mean.py`

### Class: `EWMParityGroupByMeanTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_groupby_ewm_func`
Tests GroupBy exponentially weighted moving operations with the `mean` aggregation.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/window/test_parity_groupby_expanding.py`

### Class: `GroupByExpandingParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_groupby_expanding_count`
Tests GroupBy expanding windows with the `count` aggregation.

#### `test_groupby_expanding_max`
Tests GroupBy expanding windows with the `max` aggregation.

#### `test_groupby_expanding_mean`
Tests GroupBy expanding windows with the `mean` aggregation.

#### `test_groupby_expanding_min`
Tests GroupBy expanding windows with the `min` aggregation.

#### `test_groupby_expanding_sum`
Tests GroupBy expanding windows with the `sum` aggregation.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/window/test_parity_groupby_expanding_adv.py`

### Class: `GroupByExpandingAdvParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_groupby_expanding_kurt`
Tests GroupBy expanding windows with the `kurt` (kurtosis) aggregation.

#### `test_groupby_expanding_quantile`
Tests GroupBy expanding windows with the `quantile` aggregation.

#### `test_groupby_expanding_skew`
Tests GroupBy expanding windows with the `skew` aggregation.

#### `test_groupby_expanding_std`
Tests GroupBy expanding windows with the `std` (standard deviation) aggregation.

#### `test_groupby_expanding_var`
Tests GroupBy expanding windows with the `var` (variance) aggregation.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/window/test_parity_groupby_rolling.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `RollingParityGroupTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_groupby_rolling_max`
Tests GroupBy rolling windows with the `max` aggregation.

#### `test_groupby_rolling_mean`
Tests GroupBy rolling windows with the `mean` aggregation.

#### `test_groupby_rolling_min`
Tests GroupBy rolling windows with the `min` aggregation.

#### `test_groupby_rolling_sum`
Tests GroupBy rolling windows with the `sum` aggregation.

## File: `python/pyspark/pandas/tests/connect/window/test_parity_groupby_rolling_adv.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `RollingParityGroupAdvTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_groupby_rolling_kurt`
Tests GroupBy rolling windows with the `kurt` (kurtosis) aggregation.

#### `test_groupby_rolling_quantile`
Tests GroupBy rolling windows with the `quantile` aggregation.

#### `test_groupby_rolling_skew`
Tests GroupBy rolling windows with the `skew` aggregation.

#### `test_groupby_rolling_std`
Tests GroupBy rolling windows with the `std` (standard deviation) aggregation.

#### `test_groupby_rolling_var`
Tests GroupBy rolling windows with the `var` (variance) aggregation.

## File: `python/pyspark/pandas/tests/connect/window/test_parity_groupby_rolling_count.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `RollingParityGroupCountTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_groupby_rolling_count`
Tests GroupBy rolling windows with the `count` aggregation, with a note about fixing `min_periods` behavior to match pandas.

## File: `python/pyspark/pandas/tests/connect/window/test_parity_missing.py`

### Class: `MissingParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_missing`
Verifies that calling unsupported general functions raises a `PandasNotImplementedError`. (Duplicate of test at line 36 and 104)

#### `test_missing_groupby`
Verifies that calling unsupported Expanding, Rolling, and EWM functions/properties on GroupBy objects raises appropriate errors (not implemented or deprecated).

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/pandas/tests/connect/window/test_parity_rolling.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `RollingParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_rolling_max`
Tests rolling windows with the `max` aggregation.

#### `test_rolling_mean`
Tests rolling windows with the `mean` aggregation.

#### `test_rolling_min`
Tests rolling windows with the `min` aggregation.

#### `test_rolling_sum`
Tests rolling windows with the `sum` aggregation.

## File: `python/pyspark/pandas/tests/connect/window/test_parity_rolling_adv.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `RollingParityAdvTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_rolling_kurt`
Tests rolling windows with the `kurt` (kurtosis) aggregation.

#### `test_rolling_quantile`
Tests rolling windows with the `quantile` aggregation.

#### `test_rolling_skew`
Tests rolling windows with the `skew` aggregation.

#### `test_rolling_std`
Tests rolling windows with the `std` (standard deviation) aggregation.

#### `test_rolling_var`
Tests rolling windows with the `var` (variance) aggregation.

## File: `python/pyspark/pandas/tests/connect/window/test_parity_rolling_count.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `RollingParityCountTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_rolling_count`
Tests rolling windows with the `count` aggregation, with a note about fixing `min_periods` behavior to match pandas.

## File: `python/pyspark/pandas/tests/connect/window/test_parity_rolling_error.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `RollingParityErrorTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_rolling_error`
Verifies that invalid inputs to rolling window operations raise expected errors.

## File: `python/pyspark/sql/tests/connect/arrow/test_parity_arrow.py`

### Class: `ArrowParityTests`

#### `test_arrow_map_timestamp_nulls_round_trip`
Verifies that a PyArrow Table containing a map with a string key and a UTC timestamp value, including null values, can be successfully converted to a Spark DataFrame and back to a PyArrow Table without losing data or precision.

#### `test_arrow_round_trip`
Checks the round-trip conversion from data to a Spark DataFrame and then to a PyArrow Table. It specifically handles adjusting a timezone-naive timestamp column to UTC based on the Spark session timezone configuration before comparing the output PyArrow Table with the adjusted input data.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_cached_local_relation_changing_values`
Delegates to a helper method to verify behavior when cached local relations have changing values.

#### `test_createDataFrame_arrow_column_name_encoding`
Verifies that column names are correctly encoded as strings when creating a Spark DataFrame from a PyArrow Table, both with default names and specified names.

#### `test_createDataFrame_arrow_duplicate_field_names`
Tests behavior when creating a DataFrame from a PyArrow Table with duplicate field names, verifying that it succeeds in some cases and correctly raises an exception in others.

#### `test_createDataFrame_arrow_fixed_size_binary`
Verifies that a PyArrow Table with a fixed-size binary type column is correctly converted to a Spark DataFrame with a BinaryType column.

#### `test_createDataFrame_arrow_fixed_size_list`
Verifies that a PyArrow Table with a fixed-size list type column is correctly converted to a Spark DataFrame with an ArrayType column.

#### `test_createDataFrame_arrow_large_binary`
Verifies that a PyArrow Table with a large binary type column is correctly converted to a Spark DataFrame with a BinaryType column.

#### `test_createDataFrame_arrow_large_list`
Verifies that a PyArrow Table with a large list type column is correctly converted to a Spark DataFrame with an ArrayType column.

#### `test_createDataFrame_arrow_large_list_int64_offset`
Checks for expected failure when attempting to create a DataFrame from a PyArrow Table containing a large list with an index greater than or equal to 2^31.

#### `test_createDataFrame_arrow_large_string`
Verifies that a PyArrow Table with a large string type column is correctly converted to a Spark DataFrame with a StringType column.

#### `test_createDataFrame_arrow_pandas`
Verifies that creating a DataFrame from a PyArrow Table yields the same result as creating one from a corresponding Pandas DataFrame.

#### `test_createDataFrame_arrow_respect_session_timezone`
Checks that creating a DataFrame from a PyArrow Table respects the session timezone configuration.

#### `test_createDataFrame_arrow_truncate_timestamp`
Verifies that nanosecond timestamps in a PyArrow Table are truncated to microsecond precision when converted to a Spark DataFrame.

#### `test_createDataFrame_arrow_with_array_type_nulls`
Tests the creation of a DataFrame from a PyArrow Table with array types containing nulls.

#### `test_createDataFrame_arrow_with_incorrect_schema`
Verifies that providing an incorrect schema when creating a DataFrame from a PyArrow Table results in an exception.

#### `test_createDataFrame_arrow_with_int_col_names`
Verifies that integer column names are preserved when converting to an Arrow Table and then to a Spark DataFrame.

#### `test_createDataFrame_arrow_with_map_type`
Verifies that creating a DataFrame from a PyArrow Table with a map type works correctly.

#### `test_createDataFrame_arrow_with_map_type_nulls`
Tests creation of a DataFrame from a PyArrow Table with a map type containing nulls.

#### `test_createDataFrame_arrow_with_names`
Verifies that a schema specified as a list or tuple of column names is correctly applied when creating a DataFrame from a PyArrow Table.

#### `test_createDataFrame_arrow_with_struct_type_nulls`
Tests the creation of a DataFrame from a PyArrow Table with struct types containing nulls.

#### `test_createDataFrame_does_not_modify_input`
Ensures that the input Pandas DataFrame is not modified during the process of creating a Spark DataFrame from it.

#### `test_createDataFrame_empty_partition`
Checks behavior when creating a DataFrame from a Pandas DataFrame with an empty partition.

#### `test_createDataFrame_fallback_disabled`
Tests behavior when creating a DataFrame with fallback disabled.

#### `test_createDataFrame_fallback_enabled`
Tests behavior when creating a DataFrame with fallback enabled.

#### `test_createDataFrame_pandas_column_name_encoding`
Plain language description of what the test does.

#### `test_createDataFrame_pandas_duplicate_field_names`
Delegates to a check method to verify specific behavior.

#### `test_createDataFrame_pandas_nested_timestamp`
Delegates to a check method to verify specific behavior.

#### `test_createDataFrame_pandas_respect_session_timezone`
Delegates to a check method to verify specific behavior.

#### `test_createDataFrame_pandas_with_array_type`
Delegates to a check method to verify specific behavior.

#### `test_createDataFrame_pandas_with_incorrect_schema`
Delegates to a check method to verify specific behavior.

#### `test_createDataFrame_pandas_with_int_col_names`
Delegates to a check method to verify specific behavior.

#### `test_createDataFrame_pandas_with_map_type`
Delegates to a check method to verify specific behavior.

#### `test_createDataFrame_pandas_with_names`
Plain language description of what the test does.

#### `test_createDataFrame_pandas_with_schema`
Plain language description of what the test does.

#### `test_createDataFrame_pandas_with_struct_type`
Delegates to a check method to verify specific behavior.

#### `test_createDataFrame_toggle`
Plain language description of what the test does.

#### `test_createDataFrame_udt`
Delegates to a check method to verify specific behavior.

#### `test_createDataFrame_with_category_type`
Plain language description of what the test does.

#### `test_createDataFrame_with_dictionary_type_nulls`
Plain language description of what the test does.

#### `test_createDataFrame_with_float_index`
Plain language description of what the test does.

#### `test_createDataFrame_with_int64`
Delegates to a check method to verify specific behavior.

#### `test_createDataFrame_with_ndarray`
Delegates to a check method to verify specific behavior.

#### `test_createDataFrame_with_single_data_type`
Delegates to a check method to verify specific behavior.

#### `test_createDataFrame_with_string_dtype`
Delegates to a check method to verify specific behavior.

#### `test_create_data_frame_to_arrow_day_time_internal`
Plain language description of what the test does.

#### `test_create_data_frame_to_arrow_timestamp_ntz`
Plain language description of what the test does.

#### `test_create_data_frame_to_pandas_day_time_internal`
Delegates to a check method to verify specific behavior.

#### `test_create_data_frame_to_pandas_timestamp_ntz`
Delegates to a check method to verify specific behavior.

#### `test_create_dataframe_namedtuples`
Delegates to a check method to verify specific behavior.

#### `test_filtered_frame`
Plain language description of what the test does.

#### `test_large_cached_local_relation_same_values`
Delegates to a check method to verify specific behavior.

#### `test_large_local_relation_size_limit_exceeded`
Plain language description of what the test does.

#### `test_negative_and_zero_batch_size`
Plain language description of what the test does.

#### `test_no_partition_frame`
Calls superclass method to test behavior in parity context.

#### `test_no_partition_toPandas`
Calls superclass method to test behavior in parity context.

#### `test_null_conversion`
Plain language description of what the test does.

#### `test_pandas_round_trip`
Plain language description of what the test does.

#### `test_pandas_self_destruct`
Plain language description of what the test does.

#### `test_propagates_spark_exception`
Delegates to a check method to verify specific behavior.

#### `test_schema_conversion_roundtrip`
Plain language description of what the test does.

#### `test_timestamp_dst`
Plain language description of what the test does.

#### `test_timestamp_nat`
Delegates to a check method to verify specific behavior.

#### `test_toArrow_duplicate_field_names`
Plain language description of what the test does.

#### `test_toArrow_empty_columns`
Plain language description of what the test does.

#### `test_toArrow_empty_rows`
Plain language description of what the test does.

#### `test_toArrow_error`
Plain language description of what the test does.

#### `test_toArrow_keep_utc_timezone`
Plain language description of what the test does.

#### `test_toArrow_nested_timestamp`
Verifies that converting a DataFrame with nested timestamp structures to a PyArrow Table yields a table with expected data and schema.

#### `test_toArrow_with_array_type_nulls`
Tests DataFrame conversion to PyArrow Table for array types that contain null elements.

#### `test_toArrow_with_compression_codec`
Verifies that toArrow() works correctly with different Arrow compression codecs.

#### `test_toArrow_with_compression_codec_large_dataset`
Tests toArrow() with different compression codecs using a large dataset.

#### `test_toArrow_with_map_type`
Tests DataFrame to PyArrow Table conversion for data containing map types.

#### `test_toArrow_with_map_type_nulls`
Tests DataFrame to PyArrow Table conversion for data containing map types with nulls.

#### `test_toPandas_array_of_map_empty_outer`
Checks toPandas() conversion when a DataFrame contains an array of maps where the outer array is empty.

#### `test_toPandas_arrow_toggle`
Verifies that toPandas() works accurately with and without Arrow enabled.

#### `test_toPandas_batch_order`
Verifies that batch order is preserved when converting a DataFrame to Pandas using Arrow.

#### `test_toPandas_double_nested_array_empty_outer`
Verifies toPandas() conversion when DataFrame has a double nested array with empty outer array.

#### `test_toPandas_duplicate_field_names`
Tests behavior of toPandas() on DataFrames with duplicate field names.

#### `test_toPandas_empty_columns`
Verifies toPandas() on DataFrames with no columns.

#### `test_toPandas_empty_df_arrow_enabled`
Tests behavior on an empty DataFrame with Arrow enabled.

#### `test_toPandas_empty_rows`
Verifies toPandas() on DataFrames with empty rows.

#### `test_toPandas_error`
Verifies error behavior for toPandas().

#### `test_toPandas_fallback_disabled`
Tests behavior when toPandas() fallback is disabled.

#### `test_toPandas_fallback_enabled`
Tests behavior when toPandas() fallback is enabled.

#### `test_toPandas_nested_array_with_map_empty_outer`
Verifies toPandas() conversion when DataFrame has a nested array containing maps with empty outer array.

#### `test_toPandas_nested_timestamp`
Verifies toPandas() behavior with nested timestamps.

#### `test_toPandas_respect_session_timezone`
Verifies toPandas() respects session timezone setting.

#### `test_toPandas_timestmap_tzinfo`
Tests behavior of toPandas() on timestamp columns with tzinfo.

#### `test_toPandas_triple_nested_array_empty_outer`
Checks behavior of toPandas() on DataFrames with triple nested arrays and empty outer array to ensure no SIGSEGV.

#### `test_toPandas_udt`
Tests toPandas() behavior on User Defined Types (UDTs).

#### `test_toPandas_with_array_type`
Tests toPandas() behavior on array types.

#### `test_toPandas_with_compression_codec`
Tests toPandas() behavior with different Arrow compression codec configurations.

#### `test_toPandas_with_compression_codec_large_dataset`
Tests toPandas() behavior with different compression codecs using a large dataset.

#### `test_toPandas_with_map_type`
Tests toPandas() behavior with map types.

#### `test_toPandas_with_map_type_nulls`
Tests toPandas() behavior with map types containing nulls.

#### `test_type_conversion_round_trip`
Tests round-trip conversion for a large variety of data types between Spark DataFrame and PyArrow Table schemas.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/arrow/test_parity_arrow_cogrouped_map.py`

### Class: `CogroupedMapInArrowParityTests`

#### `test_apply_in_arrow`
Tests the basic usage of applyInArrow on a grouped dataframe, verifying that the function receives a PyArrow Table and returns the expected results.

#### `test_apply_in_arrow_column_order`
Tests that applyInArrow correctly assigns columns by name even if the function returns columns in a different order than specified in the schema.

#### `test_apply_in_arrow_empty_groupby`
Tests applyInArrow with an empty groupby (global aggregation), comparing a custom normalization function in Arrow with Spark window functions.

#### `test_apply_in_arrow_not_returning_arrow_table`
Verifies that applyInArrow raises a PythonException when the user-defined function does not return a PyArrow Table or RecordBatch.

#### `test_apply_in_arrow_returning_empty_dataframe`
Tests that applyInArrow handles cases where the UDF returns an empty PyArrow Table for some groups.

#### `test_apply_in_arrow_returning_empty_dataframe_and_wrong_column_names`
Verifies that applyInArrow raises an exception when returning an empty table with column names that do not match the specified schema.

#### `test_apply_in_arrow_returning_wrong_column_names`
Verifies that applyInArrow raises an exception when the returned PyArrow Table has column names that do not match the specified schema.

#### `test_apply_in_arrow_returning_wrong_types`
Verifies that applyInArrow raises an exception when the returned PyArrow Table columns do not match the specified schema data types.

#### `test_apply_in_arrow_returning_wrong_types_positional_assignment`
Verifies that applyInArrow raises an exception when data types don't match, even with legacy positional assignment disabled.

#### `test_arrow_batch_slicing`
Tests cogroup.applyInPandas with various Arrow batch size configurations, verifying that slicing and batching work correctly.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_cogroup_apply_in_arrow_with_logging`
Tests cogroup.applyInArrow with logging enabled, verifying that worker logs are correctly captured and can be queried via python_worker_logs.

#### `test_negative_and_zero_batch_size`
Tests that setting negative or zero values for maxRecordsPerBatch doesn't cause failures, falling back to other tests for validation.

#### `test_positional_assignment_conf`
Tests that positional assignment in grouped map pandas UDFs works when the corresponding legacy configuration is enabled.

#### `test_self_join`
Tests that self-joins involving filters work correctly in Spark Connect, highlighting a case that reportedly fails in classic Spark (SPARK-47713).

#### `test_with_local_data`
SPARK-41114: Test creating a dataframe using local data

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/arrow/test_parity_arrow_grouped_map.py`

### Class: `ApplyInArrowParityTests`

#### `test_apply_in_arrow`
Tests the basic usage of applyInArrow on a grouped dataframe, verifying that the function receives a PyArrow Table and returns the expected results.

#### `test_apply_in_arrow_batching`
Tests applyInArrow with an iterator interface, verifying that multiple Arrow batches are correctly handled when the group size exceeds maxRecordsPerBatch.

#### `test_apply_in_arrow_column_order`
Tests that applyInArrow correctly assigns columns by name even if the returned table has a different column order than specified in the schema.

#### `test_apply_in_arrow_empty_groupby`
Tests applyInArrow with an empty groupby (global aggregation), comparing a custom normalization function in Arrow with Spark window functions.

#### `test_apply_in_arrow_iter_with_logging`
Tests applyInArrow with an iterator interface and logging enabled, verifying that worker logs are correctly captured.

#### `test_apply_in_arrow_not_returning_arrow_table`
Verifies that applyInArrow raises a PythonException when the user function does not return a PyArrow Table or RecordBatch.

#### `test_apply_in_arrow_partial_iteration`
Tests that applyInArrow works correctly even when the user function does not fully iterate through the input batches.

#### `test_apply_in_arrow_returning_empty_dataframe`
Tests that applyInArrow handles cases where the UDF returns an empty PyArrow Table for some groups.

#### `test_apply_in_arrow_returning_empty_dataframe_and_wrong_column_names`
Verifies that applyInArrow raises an exception when returning an empty table with column names that do not match the specified schema.

#### `test_apply_in_arrow_returning_wrong_column_names`
Verifies that applyInArrow raises an exception when the returned PyArrow Table has column names that do not match the specified schema.

#### `test_apply_in_arrow_returning_wrong_types`
Verifies that applyInArrow raises an exception when the returned PyArrow Table columns do not match the specified schema data types.

#### `test_apply_in_arrow_returning_wrong_types_positional_assignment`
Verifies that applyInArrow raises an exception when data types don't match, even with legacy positional assignment disabled.

#### `test_apply_in_arrow_with_key`
Tests that applyInArrow correctly passes both the grouping key (as a tuple of scalars) and the group data (as a Table) to the UDF.

#### `test_apply_in_arrow_with_logging`
Tests applyInArrow with logging enabled, verifying that worker logs are correctly captured.

#### `test_arrow_batch_slicing`
Tests cogroup.applyInPandas with various Arrow batch size configurations, verifying that slicing and batching work correctly.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_negative_and_zero_batch_size`
Tests that setting negative or zero values for maxRecordsPerBatch doesn't cause failures, falling back to other tests for validation.

#### `test_positional_assignment_conf`
Tests that positional assignment in grouped map pandas UDFs works when the corresponding legacy configuration is enabled.

#### `test_self_join`
Tests that self-joins involving filters work correctly in Spark Connect, highlighting a case that reportedly fails in classic Spark (SPARK-47713).

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/arrow/test_parity_arrow_map.py`

### Class: `ArrowMapParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_chain_map_in_arrow`
Tests chaining of mapInArrow operations, verifying that data flows correctly through multiple Arrow map operations.

#### `test_different_output_length`
Tests that mapInPandas can produce a different number of rows than the input, verifying handling of output dataframes with varied lengths.

#### `test_empty_iterator`
Tests that mapInPandas returns an empty result when the UDF returns an empty iterator.

#### `test_empty_rows`
Tests that mapInArrow returns an empty result when the UDF returns a RecordBatch with zero rows.

#### `test_large_variable_width_types`
Tests mapInArrow with useLargeVarTypes configuration enabled, verifying that large strings and binaries are correctly supported.

#### `test_map_in_arrow`
Tests the basic usage of mapInArrow on a dataframe, verifying that it correctly processes data in Arrow batches.

#### `test_map_in_arrow_with_barrier_mode`
Tests that mapInArrow works in both normal and barrier execution modes, verifying that the correct TaskContext type is available in the worker.

#### `test_map_in_arrow_with_limit`
Tests that mapInArrow handles queries with a limit correctly, without processing more batches than needed.

#### `test_map_in_arrow_with_logging`
Tests mapInArrow with logging enabled, verifying that worker logs are correctly captured.

#### `test_multiple_columns`
Tests mapInPandas with a dataframe containing multiple columns of different types (integer and string).

#### `test_negative_and_zero_batch_size`
Tests that setting negative or zero values for maxRecordsPerBatch doesn't cause failures, falling back to other tests for validation.

#### `test_nested_extraneous_field`
Verifies that mapInArrow raises an exception when attempting to project a nested struct with fields that do not exist in the returned Arrow data.

#### `test_nullability_narrow`
Tests that mapInArrow handles schema validation correctly when the UDF returns a non-nullable field that corresponds to a nullable field in the output schema.

#### `test_nullability_widen`
Verifies that mapInArrow raises an exception when UDF returns a nullable field that corresponds to a non-nullable field in the output schema.

#### `test_other_than_recordbatch_iter`
Verifies that mapInArrow raises an exception when UDF returns an iterator of types other than pa.RecordBatch.

#### `test_passing_metadata`
Tests that schema metadata associated with fields is correctly passed through mapInArrow.

#### `test_self_join`
Tests that self-joins involving filters work correctly in Spark Connect, highlighting a case that reportedly fails in classic Spark (SPARK-47713).

#### `test_top_level_wrong_order`
Verifies that mapInArrow raises an exception when top-level columns in the returned Arrow table do not match the expected schema order.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/arrow/test_parity_arrow_python_udf.py`

### Class: `ArrowPythonUDFParityLegacyTests`

#### `test_arrow_udf_int_to_decimal_coercion`
Tests coercion from integer to decimal types in Arrow UDFs, checking behavior with coercion enabled and disabled.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_broadcast_in_udf`
Calls superclass to test using broadcast variables inside UDFs.

#### `test_chained_udf`
Tests chaining multiple UDF calls in SQL.

#### `test_chained_udfs_with_variant`
Tests chaining UDFs where one returns a Variant and another consumes it or its parts.

#### `test_complex_input_types`
Tests that complex types like array, map, and struct can be used as inputs to Arrow UDFs.

#### `test_complex_return_types`
Tests that complex types like array, map, and struct can be used as return types for UDFs.

#### `test_datasource_with_udf`
Tests using UDFs with different data sources (file source, V1 SimpleScanSource, V2 SimpleDataSourceV2).

#### `test_day_time_interval_in_struct`
Test that DayTimeIntervalType works within StructType with Arrow UDFs.

#### `test_day_time_interval_type_casting`
Test that DayTimeIntervalType UDFs work with Arrow and preserve field specifications.

#### `test_decimal_round`
Tests precision issues with Decimal types during DataFrame creation.

#### `test_err_return_type`
Verifies that using unsupported return types (like VarcharType) with Arrow UDF raises PySparkNotImplementedError.

#### `test_err_udf_init`
Calls an internal method to check for errors during UDF initialization.

#### `test_err_udf_registration`
Verifies that trying to register a non-callable object as a UDF raises a PySparkTypeError.

#### `test_eval_type`
Verifies that creating a UDTF with useArrow=True or False yields appropriate PythonEvalType.

#### `test_file_dsv2_with_udf_filter`
Tests applying a UDF filter on a Parquet file read via V2 source when V1 sources are disabled in configuration.

#### `test_kwargs`
Tests using keyword arguments in UDF calls within both DataFrame API and SQL.

#### `test_multiple_udfs`
Tests using multiple different UDFs in a single SQL query.

#### `test_multiple_udfs_with_logging`
Tests that logs from multiple different UDFs executed in the same query are captured properly.

#### `test_named_arguments`
Tests calling UDFs using named arguments in DataFrame API and SQL.

#### `test_named_arguments_and_defaults`
Tests UDF calls with combinations of named arguments and default values.

#### `test_named_arguments_negative`
Tests negative scenarios for named arguments in UDFs, verifying expected errors for duplicates or unexpected arguments.

#### `test_nested_array`
Tests taking and returning nested arrays in UDFs.

#### `test_nested_array_input`
Tests taking nested arrays as input for Arrow-optimized UDFs.

#### `test_nested_map`
Tests taking and returning nested maps in UDFs.

#### `test_nested_struct`
Tests taking and returning nested structs in UDFs.

#### `test_non_existed_udaf`
Verifies that trying to register a non-existent Java UDAF raises an AnalysisException.

#### `test_non_existed_udf`
Verifies that trying to register a non-existent Java UDF raises an AnalysisException.

#### `test_non_existed_udf_with_sql_context`
Calls superclass to verify behavior when trying to register a non-existent Java UDF on SQLContext.

#### `test_nondeterministic_udf`
Verifies that non-deterministic UDFs are evaluated only once in chained UDF evaluations.

#### `test_nondeterministic_udf2`
Tests registering and calling non-deterministic UDFs and their behavior with pydoc.

#### `test_nondeterministic_udf3`
Calls superclass to test non-deterministic UDFs.

#### `test_nondeterministic_udf_in_aggregate`
Calls an internal method to check behavior of non-deterministic UDFs in aggregate operations.

#### `test_nonparam_udf_with_aggregate`
Tests using a UDF without parameters in a query that involves a distinct aggregation.

#### `test_num_arguments`
Tests calling UDFs with different numbers of arguments (zero vs one) and order of evaluation.

#### `test_python_udf_segfault`
Verifies that a segmentation fault in a Python UDF is caught and raises an Exception when faulthandler is enabled.

#### `test_raise_stop_iteration`
Verifies that a UDF raising StopIteration fails the query with a PythonException.

#### `test_register`
Verifies that registering a UDF with spark.udf.register works and returns a UserDefinedFunction.

#### `test_register_java_function`
Tests registering and calling Java UDFs from Python.

#### `test_register_java_udaf`
Tests registering and calling Java UDAFs (User Defined Aggregate Functions) from Python.

#### `test_same_accumulator_in_udfs`
Calls superclass to test using the same accumulator in multiple UDFs.

#### `test_single_udf_with_repeated_argument`
Tests using a single UDF call with repeated argument in SQL.

#### `test_timeout_util_with_udf`
Tests that a timeout decorator can be used to abort a UDF execution taking too long.

#### `test_type_coercion_string_to_numeric`
Tests automatic type coercion from string input to numeric output types in UDFs.

#### `test_udf`
General tests for creating and using UDFs as normal functions and decorators, with different return type specifications.

#### `test_udf2`
Tests registering a UDF and using it in a WHERE clause of an SQL query.

#### `test_udf3`
Tests registering a UDF with two arguments and calling it in SQL.

#### `test_udf_and_common_filter_in_join_condition`
Tests complex join condition involving both a Python UDF and a standard column comparison.

#### `test_udf_as_join_condition`
Tests using a UDF in a join condition where it operates on both sides of the join.

#### `test_udf_binary_type`
Calls superclass to test binary type in UDFs.

#### `test_udf_binary_type_in_nested_structures`
Calls superclass to test binary type inside nested structures in UDFs.

#### `test_udf_cache`
Verifies that UDF evaluations are correctly cached and plan shows in-memory relation usage.

#### `test_udf_daytime_interval`
Tests support for DayTimeIntervalType in Python UDFs.

#### `test_udf_defers_judf_initialization`
Calls superclass to test deferred initialization of Java UDFs.

#### `test_udf_empty_frame`
Verifies that applying a UDF on an empty DataFrame yields an empty result without errors.

#### `test_udf_globals_not_overwritten`
Verifies that global variables (like map) inside UDFs are not overwritten by external imports or closures incorrectly.

#### `test_udf_in_filter_on_top_of_join`
Verifies that a UDF can be used in a filter operation applied after a cross join.

#### `test_udf_in_filter_on_top_of_outer_join`
Verifies that a UDF can be used in a filter operation applied after a left outer join.

#### `test_udf_in_generate`
Tests using a UDF that returns an array with the explode function to generate multiple rows, verifying aggregation and exact row content.

#### `test_udf_in_join_condition`
Tests a UDF used in a join condition, verifying it triggers a cartesian product error when cross joins are disabled and succeeds when enabled.

#### `test_udf_in_left_outer_join_condition`
Tests a UDF in a left outer join condition where the condition references attributes from both sides but the UDF only references one side.

#### `test_udf_in_subquery`
Tests a UDF used within a filter in a subquery (specifically an IN subquery).

#### `test_udf_input_serialization_valuecompare_disabled`
Tests that UDFs correctly handle struct inputs and return types without failing due to value comparison issues when serialization is involved.

#### `test_udf_kill_on_timeout`
Tests that the Python worker process is terminated when it exceeds the configured idle timeout.

#### `test_udf_not_supported_in_join_condition`
Verifies that using a Python UDF in the ON clause of non-inner joins (full, left, right outer, anti, semi) raises an AnalysisException.

#### `test_udf_on_sql_context`
A parity test that delegates to the base test class implementation for testing UDFs on the SQL context.

#### `test_udf_registration_return_type_none`
Tests registering a UDF with a specified return type in the UDF definition and verifying it works in SQL.

#### `test_udf_registration_return_type_not_none`
A parity test that delegates to a helper method to verify UDF registration when a return type is explicitly provided.

#### `test_udf_registration_returns_udf`
Verifies that registering a UDF returns a function that can be used in the DataFrame API as well as in SQL expressions.

#### `test_udf_registration_returns_udf_on_sql_context`
A parity test that delegates to the base test class implementation for testing UDF registration on the SQL context.

#### `test_udf_should_not_accept_noncallable_object`
Verifies that attempting to create a UserDefinedFunction with a non-callable object raises a TypeError.

#### `test_udf_timestamp_ntz`
Tests using TimestampNTZType (Timestamp No TimeZone) in a Python UDF, verifying correctness across timezone changes.

#### `test_udf_use_arrow_and_session_conf`
Tests the interaction between the useArrow parameter in udf() and the session configuration for enabling Arrow, verifying the resulting evaluation type.

#### `test_udf_with_256_args`
Verifies that a UDF can accept a large number of arguments (specifically 256) without error.

#### `test_udf_with_aggregate_function`
Tests UDFs used in combination with distinct, filter, and group by aggregations.

#### `test_udf_with_array_type`
Tests registering and using UDFs that return array types or operate on map types in SQL queries.

#### `test_udf_with_callable`
Tests creating a UDF from a callable class instance (an object with a __call__ method).

#### `test_udf_with_char_varchar_return_type`
Verifies that attempting to use char or varchar as UDF return types raises an exception, as they are not directly supported.

#### `test_udf_with_collated_string_types`
Tests using UDFs with different collated string types, verifying that the result type matches the expected collation.

#### `test_udf_with_column_vector`
Tests UDF execution when reading from parquet files with off-heap column vectors enabled and disabled.

#### `test_udf_with_complex_variant_input`
Tests using UDFs with inputs containing complex structures with variants (struct of variant, array of variant, map of variant).

#### `test_udf_with_complex_variant_output`
Tests UDFs that return complex structures containing variant values (struct of variant, array of variant, map of variant).

#### `test_udf_with_decorator`
Tests various ways to use the @udf decorator with and without return type specifications.

#### `test_udf_with_filter_function`
Tests using a UDF as a filter condition in a DataFrame query.

#### `test_udf_with_input_file_name`
Verifies that a UDF can correctly process the result of the input_file_name() function.

#### `test_udf_with_input_file_name_for_hadooprdd`
A parity test that delegates to the base test class implementation for testing input_file_name with HadoopRDD.

#### `test_udf_with_logging`
Tests UDF execution with logging enabled, verifying that logs are correctly captured and exposed via the python_worker_logs table-valued function.

#### `test_udf_with_order_by_and_limit`
Tests using a UDF in a query that also involves order by and limit clauses.

#### `test_udf_with_partial_function`
Tests creating a UDF from a Python functools.partial function.

#### `test_udf_with_pyspark_logger`
Tests UDF execution utilizing PySparkLogger, verifying that warning logs are correctly recorded and retrievable.

#### `test_udf_with_rand`
Verifies that a UDF can be used in conjunction with the rand() function.

#### `test_udf_with_string_return_type`
Tests UDFs that return complex types specified as DDL strings (struct and array) and basic types.

#### `test_udf_with_udt`
Extensive tests of UDFs with User Defined Types (UDTs) as inputs and outputs, including chained UDF calls.

#### `test_udf_with_variant_input`
Tests a UDF that accepts a VariantType as input and returns its string representation.

#### `test_udf_with_variant_output`
Tests a UDF that returns a VariantType value.

#### `test_udf_without_arguments`
Tests registering and calling a UDF that takes no arguments.

#### `test_udf_wrapper`
Verifies that the UDF wrapper correctly preserves the docstring, original function, and return type of the wrapped function.

#### `test_use_arrow`
Tests the behavior of the useArrow parameter in UDFs, comparing execution with Arrow enabled, disabled, and inferred.

#### `test_worker_original_stdin_closed`
A parity test that delegates to the base test class implementation for verifying that the Python worker's original stdin is closed.

### Class: `ArrowPythonUDFParityNonLegacyTests`

#### `test_arrow_udf_int_to_decimal_coercion`
Tests that Arrow-optimized Python UDFs can correctly coerce integer return values to decimal types when the appropriate configuration is enabled.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_broadcast_in_udf`
A parity test that delegates to the base test class implementation for testing broadcast variables within UDFs.

#### `test_chained_udf`
Tests that UDFs can be chained together in SQL expressions (e.g., udf1(udf2(x))).

#### `test_chained_udfs_with_variant`
Tests chained UDF calls where the intermediate or final values involve VariantType in various structures.

#### `test_complex_input_types`
Tests that Arrow-optimized Python UDFs can correctly receive complex types (array, map, struct) as inputs.

#### `test_complex_return_types`
Tests that Python UDFs can correctly return complex types (array, map, struct).

#### `test_datasource_with_udf`
Tests using UDFs in filters and with columns on DataFrames loaded from file sources, simple scan sources, and data source V2.

#### `test_day_time_interval_in_struct`
Test that DayTimeIntervalType works within StructType with Arrow UDFs.

#### `test_day_time_interval_type_casting`
Test that DayTimeIntervalType UDFs work with Arrow and preserve field specifications.

#### `test_decimal_round`
Tests DataFrame creation behavior with Decimal types, verifying specific precision results.

#### `test_err_return_type`
Verifies that creating an Arrow-optimized UDF with an invalid return type (like VarcharType) raises a PySparkNotImplementedError.

#### `test_err_udf_init`
A parity test that delegates to a helper method to verify errors during UDF initialization.

#### `test_err_udf_registration`
Verifies that attempting to register a non-callable object as a UDF raises a PySparkTypeError.

#### `test_eval_type`
Tests the evaluation type of User Defined Table Functions (UDTFs) with and without Arrow optimization enabled.

#### `test_file_dsv2_with_udf_filter`
Tests using a UDF in a filter on a DataFrame read from a Parquet source using DataSource V2.

#### `test_kwargs`
Tests UDFs that accept keyword arguments (**kwargs), verifying they work in both DataFrame and SQL operations.

#### `test_multiple_udfs`
Tests using multiple distinct UDFs in a single SQL query, including chained calls.

#### `test_multiple_udfs_with_logging`
Tests using multiple UDFs that perform logging, verifying that logs from both are correctly captured.

#### `test_named_arguments`
Tests calling UDFs using named arguments in both DataFrame operations and SQL queries.

#### `test_named_arguments_and_defaults`
Tests UDFs with arguments that have default values, calling them with and without providing those arguments.

#### `test_named_arguments_negative`
Verifies that invalid uses of named arguments (duplicate assignments, unexpected keywords) raise appropriate exceptions in UDF calls.

#### `test_nested_array`
Tests UDFs with nested array inputs and outputs, verifying string representation and modification behavior.

#### `test_nested_array_input`
Tests that Arrow-optimized Python UDFs can correctly receive nested arrays as input and produce expected string representations.

#### `test_nested_map`
Tests UDFs with nested map inputs and outputs, verifying string representation and modification behavior.

#### `test_nested_struct`
Tests UDFs with nested struct inputs and outputs, verifying string representation and modification behavior.

#### `test_non_existed_udaf`
Verifies that attempting to register a non-existent Java UDAF raises an AnalysisException.

#### `test_non_existed_udf`
Verifies that attempting to register a non-existent Java function as a UDF raises an AnalysisException.

#### `test_non_existed_udf_with_sql_context`
A parity test that delegates to the base test class implementation for testing registration of non-existent UDFs with the SQL context.

#### `test_nondeterministic_udf`
Tests that non-deterministic UDFs are correctly identified and evaluated consistently in chained expressions on a single row.

#### `test_nondeterministic_udf2`
Tests registering non-deterministic UDFs and verifies that calling help() or pydoc.render_doc on them does not raise exceptions.

#### `test_nondeterministic_udf3`
A parity test that delegates to the base test class implementation for further testing of non-deterministic UDFs.

#### `test_nondeterministic_udf_in_aggregate`
A parity test that delegates to a helper method to verify the behavior of non-deterministic UDFs used in aggregations.

#### `test_nonparam_udf_with_aggregate`
Tests a UDF without parameters used in a query that has a distinct operator, verifying it returns a constant value.

#### `test_num_arguments`
Tests calling UDFs that take no arguments and UDFs that take one argument within the same select statement.

#### `test_python_udf_segfault`
Tests that with faulthandler enabled, a Python worker segmentation fault caused by a UDF triggers an exception indicating a segmentation fault.

#### `test_raise_stop_iteration`
Verifies that raising StopIteration in a UDF results in a PythonException when it is actually reached.

#### `test_register`
Tests registering an Arrow-optimized UDF with spark.udf.register and using it in SQL expressions.

#### `test_register_java_function`
Tests registering Java functions as UDFs with various ways of specifying the return type.

#### `test_register_java_udaf`
Tests registering a Java UDAF (User Defined Aggregate Function) and using it in a SQL query with group by.

#### `test_same_accumulator_in_udfs`
A parity test that delegates to the base test class implementation for testing accumulators in UDFs.

#### `test_single_udf_with_repeated_argument`
Verifies that a UDF can be called in SQL with repeated arguments (e.g., udf(1, 1)).

#### `test_timeout_util_with_udf`
Tests that using a timeout decorator on a function calling a UDF results in an exception when the UDF execution exceeds the timeout duration.

#### `test_type_coercion_string_to_numeric`
Tests that UDFs return types can coerce string inputs to numeric types (tinyint, smallint, int, bigint, double, float) when specified, raising exceptions for invalid coercions.

#### `test_udf`
Tests normal UDF function calls and decorator usage on both classic and connect contexts, comparing results.

#### `test_udf2`
Tests registering a UDF and using it within a filter clause in a SQL query.

#### `test_udf3`
Tests registering a UDF that takes two arguments and returns the result, verifying correct operation in SQL.

#### `test_udf_and_common_filter_in_join_condition`
Tests a complex join scenario involving both a Python UDF and a standard equality filter in the join condition.

#### `test_udf_as_join_condition`
Tests a join where the condition compares the outputs of two separate UDF calls.

#### `test_udf_binary_type`
A parity test that delegates to the base test class implementation for testing UDFs with binary types.

#### `test_udf_binary_type_in_nested_structures`
A parity test that delegates to the base test class implementation for testing binary types in nested structures with UDFs.

#### `test_udf_cache`
Verifies that caching a DataFrame that applies a UDF properly utilizes the cache in subsequent plans (containing InMemoryRelation).

#### `test_udf_daytime_interval`
Tests using DayTimeIntervalType in a Python UDF, verifying that it correctly passes and returns timedelta values.

#### `test_udf_defers_judf_initialization`
A parity test that delegates to the base test class implementation for verifying deferred jUDF initialization.

#### `test_udf_empty_frame`
Verifies that applying a UDF on an empty DataFrame correctly returns an empty result set without errors.

#### `test_udf_globals_not_overwritten`
Verifies that UDF execution does not inadvertently overwrite global variables (like map).

#### `test_udf_in_filter_on_top_of_join`
Tests using a UDF in a filter that is applied after performing a cross join.

#### `test_udf_in_filter_on_top_of_outer_join`
Tests a scenario where a UDF is applied to a DataFrame after a left outer join, and used in a filter condition.

#### `test_udf_in_generate`
Tests using a UDF that returns an array with the explode function inside a generate expression.

#### `test_udf_in_join_condition`
Verifies that using a UDF in a join condition that references both sides triggers a cartesian product error when cross joins are disabled, and works correctly when they are enabled.

#### `test_udf_in_left_outer_join_condition`
Tests a left outer join where the join condition involves a UDF referencing one side and a casted column from the other side.

#### `test_udf_in_subquery`
Checks the use of a UDF within a subquery filter and ensures the outer query correctly joins with the subquery result.

#### `test_udf_input_serialization_valuecompare_disabled`
Validates UDF execution on a DataFrame containing nested row structures when value comparison optimization is disabled.

#### `test_udf_kill_on_timeout`
Verifies that a UDF is terminated and an exception is raised if its execution exceeds the configured idle timeout.

#### `test_udf_not_supported_in_join_condition`
Ensures that Python UDFs in join conditions (other than inner joins) raise an AnalysisException for various outer and semi/anti join types.

#### `test_udf_on_sql_context`
Calls the parent class implementation to test UDF functionality on a SQLContext.

#### `test_udf_registration_return_type_none`
Tests registering a UDF with a specified return type but providing None for the name, then using it in a SQL query.

#### `test_udf_registration_return_type_not_none`
Executes a helper method to verify UDF registration when a non-None return type is provided.

#### `test_udf_registration_returns_udf`
Checks that registering a UDF with spark.udf.register returns a UDF object that can be used in both SQL expressions and the DataFrame API.

#### `test_udf_registration_returns_udf_on_sql_context`
Calls the parent class implementation to test UDF registration returning a UDF object on a SQLContext.

#### `test_udf_should_not_accept_noncallable_object`
Verifies that creating a UserDefinedFunction with a non-callable object (like None) raises a TypeError.

#### `test_udf_timestamp_ntz`
Tests the use of TimestampNTZType (No TimeZone) in a Python UDF, ensuring correct data type propagation and value preservation.

#### `test_udf_use_arrow_and_session_conf`
Validates how the useArrow parameter in udf() interacts with the spark.sql.execution.pythonUDF.arrow.enabled session configuration to determine the UDF evaluation type.

#### `test_udf_with_256_args`
Ensures that a UDF can handle a large number of arguments (specifically 256).

#### `test_udf_with_aggregate_function`
Tests combining UDFs with aggregation functions (like sum and distinct) in complex DataFrame transformations involving groupBy.

#### `test_udf_with_array_type`
Verifies that UDFs can correctly handle ArrayType and MapType inputs and outputs when used in SQL queries.

#### `test_udf_with_callable`
Tests creating a UDF from a class instance that implements the __call__ method.

#### `test_udf_with_char_varchar_return_type`
Ensures that using char or varchar as a UDF return type (directly or nested) raises an exception, as they are not supported for Python UDFs.

#### `test_udf_with_collated_string_types`
Validates that UDFs work correctly with various collated string types and correctly propagate collation information to the result.

#### `test_udf_with_column_vector`
Verifies that a Python UDF works correctly when reading Parquet files with both on-heap and off-heap column vector configurations enabled.

#### `test_udf_with_complex_variant_input`
Tests passing complex data types (struct, array, map) containing Variant values as input to a Python UDF.

#### `test_udf_with_complex_variant_output`
Tests returning complex data types (struct, array, map) containing Variant values from a Python UDF.

#### `test_udf_with_decorator`
Validates the usage of the @udf decorator with various return types (Integer, Double, String, Long) and function signatures.

#### `test_udf_with_filter_function`
Ensures that a Python UDF can be used within a DataFrame filter condition.

#### `test_udf_with_input_file_name`
Verifies that a Python UDF correctly receives the path of the input file when using input_file_name().

#### `test_udf_with_input_file_name_for_hadooprdd`
A parity test that delegates to the base implementation for testing input_file_name() with HadoopRDD.

#### `test_udf_with_logging`
Tests the collection and retrieval of logs (stdout, stderr, and Python logging) from a Python worker when worker logging is enabled.

#### `test_udf_with_order_by_and_limit`
Checks that a Python UDF works correctly in a query that includes orderBy and limit clauses.

#### `test_udf_with_partial_function`
Validates that a functools.partial function can be used to define a Python UDF.

#### `test_udf_with_pyspark_logger`
Specifically tests the PySparkLogger within a Python UDF and ensures logs are correctly captured.

#### `test_udf_with_rand`
Ensures that a Python UDF can take the result of the rand() function as an argument.

#### `test_udf_with_string_return_type`
Tests defining Python UDFs using string-based type specifications (e.g., "integer", "array<double>").

#### `test_udf_with_udt`
Comprehensively tests Python UDFs with User Defined Types (UDTs), including nested UDTs in arrays and chaining UDFs with UDT inputs/outputs.

#### `test_udf_with_variant_input`
Tests passing a simple Variant type as input to a Python UDF.

#### `test_udf_with_variant_output`
Tests returning a simple Variant type from a Python UDF.

#### `test_udf_without_arguments`
Verifies that a Python UDF can be registered and called without any arguments.

#### `test_udf_wrapper`
Validates that the udf wrapper correctly preserves function metadata like docstrings and return types, including for callable objects and partial functions.

#### `test_use_arrow`
Tests the useArrow parameter in the udf function, ensuring consistent results whether Arrow is explicitly enabled, disabled, or set to default.

#### `test_worker_original_stdin_closed`
A parity test checking that the original stdin is closed in the Python worker process.

### Class: `ArrowPythonUDFParityTests`

#### `test_arrow_udf_int_to_decimal_coercion`
This test verifies that Arrow UDFs correctly handle coercion from integer to decimal when enabled, and fail when disabled.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_broadcast_in_udf`
This test calls the superclass implementation to test broadcast variables in UDFs.

#### `test_chained_udf`
This test verifies that UDFs can be chained together in SQL queries.

#### `test_chained_udfs_with_variant`
This test verifies that UDFs involving variants can be chained together.

#### `test_complex_input_types`
This test verifies that UDFs can handle complex input types (array, map, struct) and return their string representation.

#### `test_complex_return_types`
This test verifies that UDFs can return complex types (array, map, struct).

#### `test_datasource_with_udf`
This test verifies that UDFs can be used with various data sources (file source, data source v2).

#### `test_day_time_interval_in_struct`
Test that DayTimeIntervalType works within StructType with Arrow UDFs.

#### `test_day_time_interval_type_casting`
Test that DayTimeIntervalType UDFs work with Arrow and preserve field specifications.

#### `test_decimal_round`
This test verifies the behavior of rounding decimals in a DataFrame.

#### `test_err_return_type`
This test verifies that an error is raised when an invalid return type (like `VarcharType`) is used with Arrow-optimized Python UDFs.

#### `test_err_udf_init`
This test calls a helper method to check error handling during UDF initialization.

#### `test_err_udf_registration`
This test verifies that an error is raised when trying to register a non-callable object as a UDF.

#### `test_eval_type`
This test verifies that the `evalType` of a UDTF is correctly identified as `SQL_TABLE_UDF` or `SQL_ARROW_TABLE_UDF` depending on whether Arrow is used.

#### `test_file_dsv2_with_udf_filter`
This test verifies that UDF filters work correctly with Data Source V2 (parquet).

#### `test_kwargs`
This test verifies that UDFs can accept keyword arguments and are correctly handled in SQL and DataFrame API.

#### `test_multiple_udfs`
This test verifies that multiple UDFs can be used in the same query.

#### `test_multiple_udfs_with_logging`
This test verifies that logging works correctly when multiple UDFs are used in the same query.

#### `test_named_arguments`
This test verifies that UDFs can be called with named arguments in various ways.

#### `test_named_arguments_and_defaults`
This test verifies that UDFs with default arguments can be called with or without providing the default arguments, and with named arguments.

#### `test_named_arguments_negative`
This test verifies error cases for named arguments in UDFs (duplicate arguments, unexpected keywords, etc.).

#### `test_nested_array`
This test verifies that UDFs can handle nested arrays as input and output.

#### `test_nested_array_input`
This test verifies that UDFs can handle nested arrays as input specifically with Arrow optimization on.

#### `test_nested_map`
This test verifies that UDFs can handle nested maps as input and output.

#### `test_nested_struct`
This test verifies that UDFs can handle nested structs as input and output.

#### `test_non_existed_udaf`
This test verifies that trying to register a non-existent Java UDAF raises an analysis exception.

#### `test_non_existed_udf`
This test verifies that trying to register a non-existent Java UDF raises an analysis exception.

#### `test_non_existed_udf_with_sql_context`
This test calls the superclass implementation to verify behavior with non-existent UDFs on SQLContext.

#### `test_nondeterministic_udf`
This test verifies that non-deterministic UDFs are evaluated only once when chained.

#### `test_nondeterministic_udf2`
This test verifies the registration and behavior of non-deterministic UDFs, including rendering documentation for them.

#### `test_nondeterministic_udf3`
This test calls the superclass implementation for non-deterministic UDF testing.

#### `test_nondeterministic_udf_in_aggregate`
This test calls a helper method to check the behavior of non-deterministic UDFs in aggregate functions.

#### `test_nonparam_udf_with_aggregate`
This test verifies that a parameterless UDF can be used in a query with aggregation.

#### `test_num_arguments`
This test verifies that UDFs can be called with different numbers of arguments and work correctly when mixed in a select statement.

#### `test_python_udf_segfault`
This test verifies that a segmentation fault in a Python UDF is handled and reported when fault handler is enabled.

#### `test_raise_stop_iteration`
This test verifies that raising `StopIteration` in a UDF is correctly handled or propagated.

#### `test_register`
This test verifies that registering a UDF returns a function that can be used and that Arrow optimization is active if configured.

#### `test_register_java_function`
This test verifies that Java functions can be registered as UDFs in various ways.

#### `test_register_java_udaf`
This test verifies that Java UDAFs can be registered and used in SQL queries.

#### `test_same_accumulator_in_udfs`
This test calls the superclass implementation to test sharing accumulators in UDFs.

#### `test_single_udf_with_repeated_argument`
This test verifies that a UDF can be called with repeated arguments.

#### `test_timeout_util_with_udf`
This test verifies that a timeout is triggered when a UDF takes too long to execute, using a timeout decorator.

#### `test_type_coercion_string_to_numeric`
This test verifies that UDFs correctly handle type coercion from string to numeric types, and fail for invalid coercions.

#### `test_udf`
This test compares behavior between PySpark Classic and PySpark Connect for UDFs used as normal functions, decorators, and with various return types.

#### `test_udf2`
This test verifies that a registered UDF can be used in a WHERE clause.

#### `test_udf3`
This test verifies that a registered UDF with two arguments works correctly and returns the expected string result.

#### `test_udf_and_common_filter_in_join_condition`
This test verifies that a UDF can be used in a join condition along with a common filter without requiring cross join to be enabled.

#### `test_udf_as_join_condition`
This test verifies that UDFs can be used as join conditions when combined with common filters.

#### `test_udf_binary_type`
This test calls the superclass implementation to test binary types in UDFs.

#### `test_udf_binary_type_in_nested_structures`
This test calls the superclass implementation to test binary types in nested structures in UDFs.

#### `test_udf_cache`
This test verifies that a DataFrame with a UDF can be cached and that subsequent queries use the cached relation.

#### `test_udf_daytime_interval`
This test verifies that `DayTimeIntervalType` is supported in Python UDFs.

#### `test_udf_defers_judf_initialization`
This test calls the superclass implementation to verify deferred initialization of Java UDFs.

#### `test_udf_empty_frame`
This test verifies that calling a UDF on an empty DataFrame returns an empty result (repeated for Arrow parity class).

#### `test_udf_globals_not_overwritten`
This test checks that global functions are not overwritten in the UDF environment (repeated for Arrow parity class).

#### `test_udf_in_filter_on_top_of_join`
This test verifies that a UDF can be used in a filter condition on top of a join operation (repeated for Arrow parity class).

#### `test_udf_in_filter_on_top_of_outer_join`
Verifies that a UDF can be used in a filter operation applied after a left outer join.

#### `test_udf_in_generate`
Tests the usage of UDFs that return array types within the explode function to generate rows.

#### `test_udf_in_join_condition`
Verifies behavior when using a UDF that references columns from both sides of a join as the join condition, checking for cartesian product errors and correct results when enabled.

#### `test_udf_in_left_outer_join_condition`
Verifies that a UDF can be used in the condition of a left outer join when it only refers to attributes from one side.

#### `test_udf_in_subquery`
Tests using a UDF inside a subquery within a WHERE clause.

#### `test_udf_input_serialization_valuecompare_disabled`
Verifies that UDFs work correctly when input rows contain nested rows (tuples) and need serialization, specifically when value comparison might be disabled.

#### `test_udf_kill_on_timeout`
Verifies that the Python worker process is terminated if a UDF execution exceeds the configured idle timeout.

#### `test_udf_not_supported_in_join_condition`
Verifies that Python UDFs are not supported in join conditions for full, left, right outer joins and left anti/semi joins, raising AnalysisException.

#### `test_udf_on_sql_context`
Calls the superclass implementation of test_udf_on_sql_context to verify UDF behavior on SQLContext.

#### `test_udf_registration_return_type_none`
Verifies registration of a function that returns an integer and calling it via SQL.

#### `test_udf_registration_return_type_not_none`
Calls an internal method to check UDF registration with non-none return type.

#### `test_udf_registration_returns_udf`
Verifies that registering a UDF returns a usable UDF object in both DataFrame API and SQL.

#### `test_udf_registration_returns_udf_on_sql_context`
Calls superclass to verify that UDF registration returns a UDF on SQLContext.

#### `test_udf_should_not_accept_noncallable_object`
Verifies that creating a UserDefinedFunction with a non-callable object raises a TypeError.

#### `test_udf_timestamp_ntz`
Tests the support of TimestampNTZType (Timestamp No TimeZone) in Python UDFs.

#### `test_udf_use_arrow_and_session_conf`
Verifies the interaction between session configuration for Arrow execution and the useArrow parameter in udf creation.

#### `test_udf_with_256_args`
Tests that a UDF can handle a large number of arguments (256).

#### `test_udf_with_aggregate_function`
Tests using UDFs in conjunction with aggregate functions like sum and distinct.

#### `test_udf_with_array_type`
Tests registering and using UDFs that take or return array types in SQL queries.

#### `test_udf_with_callable`
Verifies that a class instance with a __call__ method can be used to create a UDF.

#### `test_udf_with_char_varchar_return_type`
Verifies that using char or varchar as return types in Arrow UDFs is not supported and raises an exception.

#### `test_udf_with_collated_string_types`
Tests using UDFs with different collated string types as inputs and outputs.

#### `test_udf_with_column_vector`
Tests reading Parquet files and applying UDFs with column vector optimization enabled (offheap true/false).

#### `test_udf_with_complex_variant_input`
Tests UDFs that take complex structures containing Variant types as input.

#### `test_udf_with_complex_variant_output`
Tests UDFs that return complex structures containing Variant types as output.

#### `test_udf_with_decorator`
Tests creating UDFs using the @udf decorator with various return types.

#### `test_udf_with_filter_function`
Tests using a UDF inside a filter operation combined with another condition.

#### `test_udf_with_input_file_name`
Verifies that a UDF can access the input_file_name() of the current row.

#### `test_udf_with_input_file_name_for_hadooprdd`
Calls superclass to verify that UDFs work with input_file_name() on HadoopRDD.

#### `test_udf_with_logging`
Tests capturing logs and stdout/stderr from Python UDF workers using python_worker_logs TVF.

#### `test_udf_with_order_by_and_limit`
Verifies that UDFs work correctly when used in pipelines with order by and limit operations.

#### `test_udf_with_partial_function`
Verifies that a partial function created with functools.partial can be used to create a UDF.

#### `test_udf_with_pyspark_logger`
Verifies that logging from within a UDF using PySparkLogger works and logs are correctly propagated and readable.

#### `test_udf_with_rand`
Verifies that UDFs work correctly when applied on columns generated by rand().

#### `test_udf_with_string_return_type`
Tests creating UDFs with return types specified as DDL strings (e.g., 'integer', 'struct<...>').

#### `test_udf_with_udt`
Extensive tests for using User Defined Types (UDTs) as inputs and outputs in Python UDFs, including chained UDFs.

#### `test_udf_with_variant_input`
Tests UDFs that take Variant type as input.

#### `test_udf_with_variant_output`
Tests UDFs that return Variant type as output.

#### `test_udf_without_arguments`
Tests registering and calling a UDF that takes no arguments.

#### `test_udf_wrapper`
Verifies that udf preserves docstrings and attributes of the wrapped function.

#### `test_use_arrow`
Verifies the results are same regardless of useArrow being True, False or None when operating on array columns.

#### `test_worker_original_stdin_closed`
Calls superclass to verify worker original stdin is closed.

### Class: `UDFParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_broadcast_in_udf`
Tests that broadcast variables can be used inside a standard Python UDF.

#### `test_chained_udf`
Tests chaining of standard Python UDFs and their usage within complex SQL expressions.

#### `test_chained_udfs_with_variant`
Tests chaining of UDFs that take or return the semi-structured Variant data type.

#### `test_complex_return_types`
Tests standard Python UDFs returning complex types like arrays, maps, and structs.

#### `test_datasource_with_udf`
Tests using Python UDFs in projections and filters over various data sources (CSV, SimpleScanSource, SimpleDataSourceV2).

#### `test_err_udf_init`
Tests error handling during UDF initialization.

#### `test_err_udf_registration`
Tests error handling when attempting to register an invalid UDF via catalog.

#### `test_file_dsv2_with_udf_filter`
Tests using a Python UDF as a filter on a Parquet file read through Data Source V2.

#### `test_kwargs`
Tests using keyword arguments when invoking standard Python UDFs from Python or SQL.

#### `test_multiple_udfs`
Tests using multiple distinct standard Python UDFs in the same query or chaining them together.

#### `test_multiple_udfs_with_logging`
Tests using multiple standard Python UDFs with logging enabled in a single query.

#### `test_named_arguments`
Tests invoking standard Python UDFs with named arguments in both DataFrame and SQL APIs.

#### `test_named_arguments_and_defaults`
Tests invoking standard Python UDFs that have default arguments, with and without naming them.

#### `test_named_arguments_negative`
Verifies error handling when standard Python UDFs are invoked with invalid or duplicate named arguments.

#### `test_nested_array`
Tests handling of nested arrays as both inputs to and outputs from standard Python UDFs.

#### `test_nested_map`
Tests handling of nested maps as both inputs to and outputs from standard Python UDFs.

#### `test_nested_struct`
Tests handling of nested structs as both inputs to and outputs from standard Python UDFs.

#### `test_non_existed_udaf`
Verifies that attempting to register a non-existent Java UDAF raises an AnalysisException.

#### `test_non_existed_udf`
Verifies that attempting to register a non-existent Java UDF raises an AnalysisException.

#### `test_non_existed_udf_with_sql_context`
Fallback test verifying behavior when attempting to use a non-existent UDF.

#### `test_nondeterministic_udf`
Verifies that non-deterministic UDFs are only evaluated once in chained expressions.

#### `test_nondeterministic_udf2`
Tests catalog registration and introspection (help/doc rendering) of non-deterministic UDFs.

#### `test_nondeterministic_udf3`
Fallback test for another non-deterministic UDF scenario.

#### `test_nondeterministic_udf_in_aggregate`
Verifies behavior when non-deterministic UDFs are used within an aggregation operation.

#### `test_nonparam_udf_with_aggregate`
Tests usage of non-parameterized UDFs alongside aggregation/distinct operations.

#### `test_num_arguments`
Verifies that standard Python UDFs with varying numbers of arguments are evaluated correctly.

#### `test_python_udf_segfault`
Tests that enabling faulthandler correctly exposes segmentation faults in Python UDFs.

#### `test_raise_stop_iteration`
Tests that raising StopIteration within a standard Python UDF is handled and bubbles up as a PythonException.

#### `test_register_java_function`
Tests catalog registration and execution of Java-defined scalar functions.

#### `test_register_java_udaf`
Tests catalog registration and execution of Java-defined aggregate functions.

#### `test_same_accumulator_in_udfs`
Fallback test for accumulator usage in UDFs.

#### `test_single_udf_with_repeated_argument`
Verifies that standard Python UDFs are evaluated correctly even if same argument is repeated.

#### `test_timeout_util_with_udf`
Tests that UDF execution respects custom timeouts applied to DataFrame operations.

#### `test_udf`
Tests basic usage of standard Python UDFs via DataFrame and SQL APIs, as well as decorator style.

#### `test_udf2`
Verifies catalog registration and usage of standard Python UDFs within standard SQL queries.

#### `test_udf3`
Verifies that UDF arguments and catalog return objects behave deterministically.

#### `test_udf_and_common_filter_in_join_condition`
Tests complex query plans involving both standard Python UDFs and common columns as join conditions.

#### `test_udf_as_join_condition`
Tests usage of standard Python UDFs as join conditions between dataframes.

#### `test_udf_binary_type`
Fallback test for binary type handling in standard Python UDFs.

#### `test_udf_binary_type_in_nested_structures`
Fallback test for nested binary type handling in standard Python UDFs.

#### `test_udf_cache`
Tests that standard Python UDF executions correctly respect and utilize Spark's in-memory caching.

#### `test_udf_daytime_interval`
Tests standard Python UDFs that accept and return DayTimeIntervalType objects.

#### `test_udf_defers_judf_initialization`
Fallback test for lazy initialization of underlying Java UDF instances.

#### `test_udf_empty_frame`
This test verifies that calling a UDF on an empty DataFrame returns an empty result.

#### `test_udf_globals_not_overwritten`
This test checks that global functions (like `map`) are not overwritten or shadowed in the UDF environment by modules like `itertools`.

#### `test_udf_in_filter_on_top_of_join`
This test verifies that a UDF can be used in a filter condition on top of a join operation, specifically testing a regression where this failed.

#### `test_udf_in_filter_on_top_of_outer_join`
This test verifies that a UDF can be used in a filter after a left outer join.

#### `test_udf_in_generate`
This test verifies the correct behavior of UDFs used within a `generate` operation (like `explode`). It tests with different array generation logic and checks the results.

#### `test_udf_in_join_condition`
This test verifies that using a UDF in a join condition correctly triggers an error when cross joins are disabled, and works when they are enabled, due to attributes being pulled from both sides of the join.

#### `test_udf_in_left_outer_join_condition`
This test verifies that a UDF can be used in a left outer join condition when it only references attributes from one side.

#### `test_udf_in_subquery`
This test verifies that a UDF can be used within a subquery in a SQL statement.

#### `test_udf_input_serialization_valuecompare_disabled`
This test verifies that UDFs work correctly with tuple/struct inputs when value comparison is disabled.

#### `test_udf_kill_on_timeout`
This test verifies that a Python worker process is terminated when a UDF exceeds the configured idle timeout.

#### `test_udf_not_supported_in_join_condition`
This test verifies that Python UDFs are not supported in join conditions for join types other than inner join, and raises appropriate exceptions.

#### `test_udf_on_sql_context`
This test calls the superclass implementation of `test_udf_on_sql_context`. It likely tests UDF usage via the old SQLContext.

#### `test_udf_registration_return_type_none`
This test verifies that a UDF can be registered with a specified return type and used in SQL, and it is considered deterministic by default.

#### `test_udf_registration_return_type_not_none`
This test calls a helper method to check UDF registration with a specific return type.

#### `test_udf_registration_returns_udf`
This test verifies that registering a UDF returns a function that can be used in both `selectExpr` and `select`.

#### `test_udf_registration_returns_udf_on_sql_context`
This test calls the superclass implementation of `test_udf_registration_returns_udf_on_sql_context`.

#### `test_udf_should_not_accept_noncallable_object`
This test verifies that creating a `UserDefinedFunction` with a non-callable object raises a `TypeError`.

#### `test_udf_timestamp_ntz`
This test verifies that `TimestampNTZType` works correctly as input and output for Python UDFs.

#### `test_udf_with_256_args`
This test verifies that a UDF can accept up to 256 arguments.

#### `test_udf_with_aggregate_function`
This test verifies that UDFs can be used in combination with aggregate functions in a query.

#### `test_udf_with_array_type`
This test verifies that UDFs can return array types and accept array types as input.

#### `test_udf_with_callable`
This test verifies that a class with a `__call__` method can be used as a UDF.

#### `test_udf_with_char_varchar_return_type`
This test verifies that using char/varchar return types in UDFs raises exceptions (likely because they are not fully supported or require specific handling in this context).

#### `test_udf_with_collated_string_types`
This test verifies that UDFs work correctly with collated string types, preserving the collation in the result.

#### `test_udf_with_column_vector`
This test verifies that UDFs work correctly when reading from Parquet files with column vectors, both on-heap and off-heap.

#### `test_udf_with_complex_variant_input`
This test verifies that UDFs can handle complex types containing variants (struct of variant, array of variant, map of variant) as input.

#### `test_udf_with_complex_variant_output`
This test verifies that UDFs can return complex types containing variants.

#### `test_udf_with_decorator`
This test verifies that UDFs can be defined using the `@udf` decorator with various return types and syntaxes.

#### `test_udf_with_filter_function`
This test verifies that UDFs can be used as filters in combination with other conditions.

#### `test_udf_with_input_file_name`
This test verifies that a UDF can be passed the result of `input_file_name()` and receive the correct path.

#### `test_udf_with_input_file_name_for_hadooprdd`
This test calls the superclass implementation for testing `input_file_name` with HadoopRDD.

#### `test_udf_with_logging`
This test verifies the Python worker logging feature, checking both when it's disabled (raises error on accessing logs TVF) and when enabled (logs are collected and accessible via TVF).

#### `test_udf_with_order_by_and_limit`
This test verifies that a UDF works correctly when used in a query with `orderBy` and `limit`.

#### `test_udf_with_partial_function`
This test verifies that a partial function can be used as a UDF.

#### `test_udf_with_pyspark_logger`
This test verifies that `PySparkLogger` can be used inside a UDF and its logs are correctly captured when worker logging is enabled.

#### `test_udf_with_rand`
This test verifies that a UDF can be called with the result of `rand()`.

#### `test_udf_with_string_return_type`
This test verifies that UDFs can return various types (integer, struct, array) defined by string DDL.

#### `test_udf_with_udt`
This test verifies that UDFs can accept and return User Defined Types (UDTs) in various combinations (chains).

#### `test_udf_with_variant_input`
This test verifies that UDFs can handle Variant types as input.

#### `test_udf_with_variant_output`
This test verifies that UDFs can return Variant types.

#### `test_udf_without_arguments`
This test verifies that a UDF without arguments can be registered and used in SQL.

#### `test_udf_wrapper`
This test verifies that `udf` correctly wraps functions, classes, and partial functions, preserving docstrings and setting correct properties.

#### `test_worker_original_stdin_closed`
This test calls the superclass implementation to verify that the worker's original stdin is closed.

## File: `python/pyspark/sql/tests/connect/arrow/test_parity_arrow_udf.py`

### Class: `ArrowPythonUDFParityTests`

#### `test_agg_arrow_udf_with_specified_eval_type`
Tests an Arrow-based aggregate UDF with a specific evaluation type (GROUPED_AGG) by calculating the maximum value of a column in a Spark DataFrame and comparing it with the result from a standard SQL expression.

#### `test_arrow_udf_basic`
Validates the basic creation and metadata of Arrow UDFs, specifically checking that the returnType and evalType (set to SQL_SCALAR_ARROW_UDF) are correctly assigned for different data types like DoubleType and VariantType.

#### `test_arrow_udf_basic_with_return_type_string`
Verifies that Arrow UDFs can be correctly initialized using string-based type definitions (e.g., 'double', 'variant') for their return types.

#### `test_arrow_udf_day_time_interval_type`
Tests the handling of DayTimeIntervalType within an Arrow UDF, ensuring that datetime.timedelta objects are correctly passed into the UDF and the resulting DataFrame maintains the expected schema.

#### `test_arrow_udf_decorator`
Confirms that the @arrow_udf decorator correctly registers Python functions as Arrow UDFs with the specified return type and evaluation type.

#### `test_arrow_udf_decorator_with_return_type_string`
Verifies that the @arrow_udf decorator works correctly when the return type is provided as a string, handling both scalar and structured return types.

#### `test_arrow_udf_timestamp_ntz`
Tests Arrow UDF support for TimestampNTZType (No TimeZone), ensuring timestamps are correctly handled without timezone conversion regardless of session settings.

#### `test_arrow_udf_wrong_arg`
Validates error handling for Arrow UDFs, ensuring appropriate exceptions are raised for invalid types, missing parameters, or unsupported zero-argument signatures.

#### `test_scalar_arrow_udf_with_specified_eval_type`
Tests scalar Arrow UDFs using both standard and iterator-based approaches, ensuring they correctly perform element-wise operations on Spark DataFrame columns.

#### `test_time_zone_against_map_in_arrow`
Checks timezone handling in Arrow UDFs and mapInArrow operations, verifying that timestamps retain their session timezone information when passed through the Arrow translation layer.

## File: `python/pyspark/sql/tests/connect/arrow/test_parity_arrow_udf_grouped_agg.py`

### Class: `GroupedAggArrowPythonUDFParityTests`

#### `test_0_args`
Tests zero-argument UDFs to ensure they default to the SQL_BATCHED_UDF evaluation type when Arrow is disabled and return expected results for each row.

#### `test_array_type`
Validates a Grouped Aggregate Pandas UDF that returns an array of doubles, ensuring it correctly applies over a window.

#### `test_arrow_batch_slicing`
Tests applyInPandas behavior with Arrow batching limits (maxRecordsPerBatch, maxBytesPerBatch), verifying correct partitioning and processing during cogroup operations.

#### `test_basic`
Verifies basic grouped aggregation using Pandas UDFs, comparing a custom weighted mean UDF against standard Spark mean aggregation.

#### `test_complex_agg_collect_as_map`
Tests an Arrow aggregate UDF that collects two columns into a map, validating it against Spark's built-in map_from_arrays and collect_list functions.

#### `test_complex_agg_collect_list`
Tests an Arrow aggregate UDF that collects values into a sorted list, comparing it with Spark's native sort_array and collect_list functions.

#### `test_complex_agg_collect_set`
Tests an Arrow aggregate UDF that collects unique values into a sorted list, validating against Spark's sort_array and collect_set functions.

#### `test_complex_agg_min_max_struct`
Tests an Arrow aggregate UDF returning a struct with min and max values, comparing it with Spark's native struct, min, and max functions.

#### `test_complex_expressions`
Tests complex Spark pipelines combining multiple UDF types (Python, Pandas scalar, and group aggregate) in withColumn and aggregate operations, ensuring correctness through SQL comparisons.

#### `test_complex_groupby`
Validates a complex groupby.apply operation using a GROUPED_MAP Pandas UDF to normalize data within groups, comparing the result with a Pandas-only operation.

#### `test_grouped_agg_arrow_udf_with_logging`
Tests an Arrow-based grouped aggregate UDF that includes logging within the UDF, verifying that logs are correctly captured and accessible via spark.tvf.python_worker_logs().

#### `test_grouped_with_empty_partition`
Tests groupBy().apply() with a PandasUDFType.GROUPED_MAP UDF on a DataFrame that has been repartitioned such that some partitions might be empty.

#### `test_grouped_without_group_by_clause`
Tests a GROUPED_AGG Pandas UDF applied to a DataFrame without an explicit groupBy clause, comparing results from both the DataFrame API and Spark SQL.

#### `test_input_output_variant`
Tests Arrow-based UDFs that take and return variant types, ensuring the internal structure of the variant (metadata and value fields as binary) is as expected.

#### `test_invalid_args`
A wrapper test that calls check_invalid_args() to verify error handling for incorrect UDF arguments.

#### `test_kwargs`
Tests the ability to use keyword arguments with Python UDFs, including registering them in the catalog and calling them via SQL and the DataFrame API, while also checking for duplicate or unexpected positional arguments.

#### `test_manual`
Manually verifies the results of various Pandas grouped aggregate UDFs (sum, mean, mean on array) against expected data created via createDataFrame.

#### `test_mixed_sql`
Tests a Pandas aggregate UDF used within a window function, comparing the results against Spark's built-in sf.mean windowed aggregation.

#### `test_multiple_udfs`
Tests registering and calling multiple simple Python UDFs, including nested UDF calls, within Spark SQL.

#### `test_named_arguments`
Tests calling a Python UDF using named arguments (keyword arguments) from both the DataFrame API and Spark SQL.

#### `test_named_arguments_and_defaults`
Tests Python UDFs that have default parameter values, calling them with and without the optional arguments using both named and positional styles.

#### `test_named_arguments_negative`
Tests error cases for UDF named arguments, such as duplicate assignments, unexpected positional arguments, or multiple values for the same argument.

#### `test_no_predicate_pushdown_through`
Ensures that predicates involving Pandas grouped aggregate UDFs are NOT pushed down through the aggregation, verifying correct filtering behavior.

#### `test_register_vectorized_udf_basic`
Tests registering and using basic vectorized (Pandas) UDFs of both SCALAR and SCALAR_ITER types, ensuring they work correctly when called via SQL or the DataFrame API.

#### `test_retain_group_columns`
Tests the effect of the spark.sql.retainGroupColumns configuration on the output of grouped aggregations using Pandas UDFs.

#### `test_return_numpy_scalar`
Verifies that Arrow-based UDFs can return NumPy scalar types (like np.int64, np.float64) and that they are correctly converted to Spark types.

#### `test_return_type_coercion`
Tests the automatic coercion of return types in Arrow-based UDFs, such as from long to int, and ensures that overflows during coercion result in an exception.

#### `test_time_min`
Tests an Arrow-based grouped aggregate UDF that computes the minimum value for TIME types, comparing results against Spark's built-in sf.min.

#### `test_unsupported_return_types`
Verifies that certain complex return types (like an array of year-month intervals) are correctly identified as unsupported for Arrow UDFs, throwing a NotImplementedError.

#### `test_iterator_grouped_agg_single_column`
Test iterator API for grouped aggregation with single column.

#### `test_iterator_grouped_agg_sql_multiple_columns`
Test iterator API for grouped aggregation with multiple columns in SQL.

#### `test_iterator_grouped_agg_sql_single_column`
Test iterator API for grouped aggregation with single column in SQL.

## File: `python/pyspark/sql/tests/connect/arrow/test_parity_arrow_udf_scalar.py`

### Class: `ScalarArrowPythonUDFParityTests`

#### `test_arrow_iter_udf_single_column`
Tests a single-column Arrow UDF of type SCALAR_ITER (using iterators of Arrow arrays), verifying it produces the same results as a basic column addition.

#### `test_arrow_iter_udf_three_columns`
Tests an Arrow UDF that processes an iterator of triplets of pyarrow Arrays, returning their product.

#### `test_arrow_iter_udf_two_columns`
Tests an Arrow UDF that processes an iterator of pairs of pyarrow Arrays, returning their product.

#### `test_arrow_udf_chained`
Verifies that scalar and iterator-based Arrow UDFs can be chained together in various combinations.

#### `test_arrow_udf_chained_ii`
Tests deep chaining of three Arrow UDFs (mixing scalar and iterator types) to ensure correct value propagation.

#### `test_arrow_udf_chained_iii`
Tests complex chaining where a binary Arrow UDF takes inputs from two other unary Arrow UDFs, testing both scalar and iterator modes.

#### `test_arrow_udf_chained_struct_type`
Verifies that Arrow UDFs can correctly process and return StructTypes when chained together.

#### `test_arrow_udf_datatype_string`
Validates that Arrow UDFs support a wide variety of Spark data types (string, int, long, float, double, decimal, boolean) using string type identifiers.

#### `test_arrow_udf_empty_partition`
Ensures that Arrow UDFs correctly handle partitions that contain no data.

#### `test_arrow_udf_input_arrow_array_struct`
Tests an Arrow UDF that takes an array of structs as input and returns it, verifying the pyarrow Array structure is preserved.

#### `test_arrow_udf_input_dates`
Verifies that an Arrow UDF can correctly extract components (like year) from pyarrow Date32Array input.

#### `test_arrow_udf_input_nested_arrays`
Tests the ability of an Arrow UDF to process deeply nested arrays and return a string representation of the complex structure.

#### `test_arrow_udf_input_nested_maps`
Validates that Arrow UDFs can correctly handle input data containing nested maps.

#### `test_arrow_udf_input_output_nested_structs`
Verifies that an Arrow UDF can correctly access and return a nested field from a complex StructArray input.

#### `test_arrow_udf_input_times`
Tests an Arrow UDF extracting temporal components (seconds) from pyarrow Time64Array input.

#### `test_arrow_udf_input_timestamps`
Validates that Arrow UDFs correctly handle pyarrow TimestampArray inputs and can extract specific time components.

#### `test_arrow_udf_input_variant`
Tests Arrow UDF support for the Variant type, verifying it is received as a StructArray containing metadata and value binary arrays.

#### `test_arrow_udf_kwargs`
Verifies that Arrow UDFs correctly handle variable keyword arguments (**kwargs) when called via DataFrame API or Spark SQL.

#### `test_arrow_udf_named_arguments`
Validates the use of named arguments when invoking Arrow UDFs from both Python and SQL interfaces.

#### `test_arrow_udf_named_arguments_and_defaults`
Tests Arrow UDFs with default argument values, ensuring they work correctly when arguments are omitted or passed by name.

#### `test_arrow_udf_named_arguments_negative`
Tests error handling for invalid Arrow UDF calls, such as duplicate named arguments or unknown keyword arguments.

#### `test_arrow_udf_null_array`
Verifies that an Arrow UDF can correctly handle and return arrays containing null values, using both SCALAR and SCALAR_ITER types.

#### `test_arrow_udf_null_binary`
Tests Arrow UDFs with binary data containing nulls, ensuring the output matches the input when using a simple identity lambda for both scalar and iterative UDF types.

#### `test_arrow_udf_null_boolean`
Ensures Arrow UDFs (scalar and iterative) correctly process boolean columns with null entries, maintaining data integrity through the transformation.

#### `test_arrow_udf_null_byte`
Validates that Arrow UDFs can handle byte-type data with nulls, confirming parity between the input DataFrame and the UDF's output.

#### `test_arrow_udf_null_decimal`
Checks the ability of Arrow UDFs to process decimal types with nulls and a specific precision/scale, verifying consistent results for both scalar and iterative implementations.

#### `test_arrow_udf_null_double`
Tests handling of double-precision floating-point numbers with nulls in Arrow UDFs, ensuring the identity function preserves the original data.

#### `test_arrow_udf_null_float`
Verifies that Arrow UDFs correctly handle single-precision floats and null values.

#### `test_arrow_udf_null_int`
Confirms that Arrow UDFs can process integer columns with null values without data loss or errors.

#### `test_arrow_udf_null_long`
Validates Arrow UDF support for long (64-bit integer) data types that include null entries.

#### `test_arrow_udf_null_short`
Ensures Arrow UDFs correctly handle short (16-bit integer) data types with nulls.

#### `test_arrow_udf_null_string`
Tests Arrow UDFs with string data containing nulls, verifying that both scalar and iterative UDFs correctly pass through the data.

#### `test_arrow_udf_output_dates`
Verifies that an Arrow UDF can correctly construct and return date values by processing year, month, and day columns.

#### `test_arrow_udf_output_nested_arrays`
Tests the ability of an Arrow UDF to return nested array structures by splitting strings into lists of lists.

#### `test_arrow_udf_output_nested_structs`
Validates that Arrow UDFs can return complex nested struct types, ensuring proper schema and data mapping for hierarchical data.

#### `test_arrow_udf_output_structs`
Confirms that Arrow UDFs can correctly output simple struct types composed of multiple fields.

#### `test_arrow_udf_output_times`
Tests an Arrow UDF that constructs and returns time values from hour, minute, and second components.

#### `test_arrow_udf_output_timestamps_ltz`
Verifies that an Arrow UDF can correctly produce timezone-aware timestamps (LTZ) using the session's timezone.

#### `test_arrow_udf_output_timestamps_ntz`
Ensures Arrow UDFs can correctly output timestamps without timezone information (NTZ), accurately mapping the components to a timestamp array.

#### `test_arrow_udf_output_variant`
Tests Arrow UDFs (both scalar and iterative) that output the Variant type, verifying they can handle the underlying binary value and metadata structures.

#### `test_arrow_udf_tokenize`
Validates an Arrow UDF that performs string tokenization using PyArrow's compute functions, returning an array of strings.

#### `test_catalog_register_arrow_udf_basic`
Verifies that scalar Arrow UDFs (both standard and iterative) can be registered with the catalog and used in DataFrames and SQL.

#### `test_catalog_register_nondeterministic_arrow_udf`
Verifies that non-deterministic Arrow UDFs can be registered with the catalog and that their properties are preserved.

#### `test_nondeterministic_arrow_udf`
Ensures that non-deterministic Arrow UDFs are evaluated only once in chained evaluations.

#### `test_nondeterministic_arrow_udf_in_aggregate`
Verifies that using a non-deterministic Arrow UDF in an aggregate function raises an exception.

#### `test_return_type_coercion`
Tests return type coercion in Arrow UDFs and checks for overflow errors.

#### `test_scalar_arrow_udf_with_logging`
Verifies that logging within a scalar Arrow UDF works and that logs can be retrieved.

#### `test_scalar_iter_arrow_udf_with_logging`
Verifies that logging within an iterative scalar Arrow UDF works and that logs are captured.

#### `test_scalar_iter_arrow_udf_with_single_output_batch`
Verifies that an iterative scalar Arrow UDF can process multiple input batches and yield a single output batch.

#### `test_udf_register_arrow_udf_basic`
Verifies that scalar Arrow UDFs can be registered using spark.udf.register and used in DataFrames and SQL.

#### `test_udf_register_nondeterministic_arrow_udf`
Verifies that non-deterministic Arrow UDFs can be registered using spark.udf.register and that their properties are preserved.

#### `test_unsupported_return_types`
Verifies that attempting to create an Arrow UDF with an unsupported return type raises a NotImplementedError.

## File: `python/pyspark/sql/tests/connect/arrow/test_parity_arrow_udf_window.py`

### Class: `WindowArrowPythonUDFParityTests`

#### `test_array_type`
Verifies that a grouped aggregate Pandas UDF returning an array type works correctly in a window function.

#### `test_arrow_batch_slicing`
Verifies the behavior of applyInPandas on co-grouped DataFrames under different batch size configurations.

#### `test_bounded_mixed`
Verifies that bounded window operations using Pandas UDFs yield the same results as native Spark functions.

#### `test_bounded_simple`
Tests aggregate Pandas UDFs in bounded window functions, comparing against native Spark functions.

#### `test_complex_window_collect_as_map`
Verifies that an Arrow UDF returning a map type works correctly in a window function.

#### `test_complex_window_collect_list`
Verifies that an Arrow UDF returning an array type works correctly in a window function.

#### `test_complex_window_collect_set`
Verifies that an Arrow UDF returning a set of unique elements works correctly in a window function.

#### `test_complex_window_min_max_struct`
Verifies that an Arrow UDF returning a struct type works correctly in a window function.

#### `test_growing_window`
Verifies that Pandas UDFs work correctly in growing window frames.

#### `test_invalid_args`
Verifies that invalid arguments to a window function are handled correctly, likely by raising an appropriate error.

#### `test_kwargs`
Tests the usage of keyword arguments with UDFs, including registration and querying via Spark SQL and DataFrame API, and ensures duplicate or unexpected positional arguments raise AnalysisException.

#### `test_mixed_sql`
Compares the result of a windowed Pandas UDF with a built-in Spark SQL window function (mean) on the same window to ensure functional parity.

#### `test_mixed_sql_and_udf`
Validates mixing built-in SQL window functions and window UDFs in the same expression, chaining them, and using them across multiple projections.

#### `test_mixed_udf`
Tests the mixture and chaining of multiple regular UDFs and Pandas UDFs (scalar and iterator types) in various combinations within a single projection or expression.

#### `test_multiple_udfs`
Registers and tests multiple lambda-based UDFs, verifying they work correctly when used together or nested in Spark SQL queries.

#### `test_named_arguments`
Confirms that UDFs can be called using named (keyword) arguments in both the DataFrame API and Spark SQL, ensuring correct result matching.

#### `test_named_arguments_negative`
Tests error cases for named arguments in UDFs, such as duplicate parameter assignment, unexpected positional arguments, and multiple values for the same argument.

#### `test_replace_existing`
Verifies that a windowed Pandas UDF correctly computes results on an unbounded window, compared against the built-in sf.mean.

#### `test_return_numpy_scalar`
Validates that Arrow UDFs can correctly return NumPy scalar types (e.g., np.int64) and match the results of built-in Spark aggregate functions.

#### `test_return_type_coercion`
Tests the automatic coercion of return types in Arrow UDFs (e.g., long to int) and ensures that overflows during coercion raise an exception.

#### `test_shrinking_window`
Validates windowed Pandas UDFs on shrinking row and range windows, comparing the results against built-in Spark SQL window functions.

#### `test_simple`
Tests the Spark Connect retry policy by simulating internal gRPC errors and verifying the number of attempts and exceptions.

#### `test_sliding_window`
Validates windowed Pandas UDFs on sliding row and range windows, comparing results against built-in Spark SQL window functions.

#### `test_time_min`
Tests grouped aggregation with an Arrow UDF on TIME types, verifying it correctly computes the minimum time value compared to built-in Spark functions.

#### `test_window_arrow_udf_with_logging`
Verifies that logging within an Arrow UDF works correctly and that logs can be retrieved and validated using python_worker_logs.

#### `test_without_partitionBy`
Validates windowed Pandas UDFs on unpartitioned windows, ensuring they match built-in Spark window function results.

## File: `python/pyspark/sql/tests/connect/arrow/test_parity_arrow_udtf.py`

### Class: `ArrowUDTFParityTests`

#### `test_arrow_udtf_blocks_analyze_method_none_return_type`
Ensures that Arrow UDTFs cannot define an analyze method if no return type is provided, raising INVALID_ARROW_UDTF_WITH_ANALYZE.

#### `test_arrow_udtf_blocks_analyze_method_with_return_type`
Ensures that Arrow UDTFs cannot define both a returnType and an analyze method, raising INVALID_UDTF_BOTH_RETURN_TYPE_AND_ANALYZE.

#### `test_arrow_udtf_data_conversion_error`
Tests that data conversion errors in Arrow UDTFs (e.g., returning an incorrect format) are correctly identified and wrapped in UDTF_ARROW_DATA_CONVERSION_ERROR.

#### `test_arrow_udtf_error_invalid_arrow_type`
Verifies that an error is raised when an Arrow UDTF yields a non-Arrow table object.

#### `test_arrow_udtf_error_mismatched_schema`
Verifies that an error is raised when the schema of the yielded Arrow table does not match the UDTF's declared return type.

#### `test_arrow_udtf_error_not_iterator`
Verifies that an error is raised when the `eval` method of an Arrow UDTF returns a table instead of an iterator.

#### `test_arrow_udtf_error_wrong_yield_type`
Verifies that an error is raised when an Arrow UDTF yields a dictionary instead of an Arrow table.

#### `test_arrow_udtf_lateral_join_disallowed`
Confirms that lateral joins are explicitly disallowed for Arrow UDTFs and raise a specific error.

#### `test_arrow_udtf_lateral_join_with_table_argument_disallowed`
Ensures that lateral joins involving Arrow UDTFs with `TABLE` arguments are also disallowed, triggering standard Spark validation errors.

#### `test_arrow_udtf_partition_by_all_columns`
Tests an Arrow UDTF that uses `PARTITION BY` and `ORDER BY` on a `TABLE` argument, specifically using `SkipRestOfInputTableException` to limit results per partition.

#### `test_arrow_udtf_partition_by_single_partition_multiple_input_partitions`
Verifies that data from multiple input partitions is correctly consolidated into a single UDTF partition when `PARTITION BY` uses a constant key.

#### `test_arrow_udtf_partition_column_removal`
Checks that Spark's internal partition expression columns are correctly removed before the data is passed to the Arrow UDTF's `eval` method.

#### `test_arrow_udtf_record_batch_iterator`
Validates that Arrow UDTFs can yield `pa.RecordBatch` objects in addition to `pa.Table` objects.

#### `test_arrow_udtf_scalar_args_only`
Tests an Arrow UDTF that takes only scalar arguments (passed as Arrow arrays) and yields results as an Arrow table.

#### `test_arrow_udtf_sql_conditional_yield`
Verifies that an Arrow UDTF can conditionally yield rows based on logic in the `eval` method when called from SQL.

#### `test_arrow_udtf_sql_empty_result`
Confirms that an Arrow UDTF can yield zero results for certain inputs, producing an empty DataFrame.

#### `test_arrow_udtf_sql_with_aggregation`
Tests an Arrow UDTF that performs manual aggregation (counting categories) on an input array argument.

#### `test_arrow_udtf_sql_with_struct_output`
Validates that Arrow UDTFs can return complex types like `struct` by yielding Arrow struct arrays.

#### `test_arrow_udtf_table_argument_with_regular_udtf_lateral_join_allowed`
Ensures that a lateral join between an Arrow UDTF (with a `TABLE` argument) and a regular Python UDTF is permitted.

#### `test_arrow_udtf_table_partition_by_multiple_columns`
Tests an Arrow UDTF that partitions its `TABLE` argument by multiple columns and aggregates results across each partition.

#### `test_arrow_udtf_table_partition_by_single_column`
Tests an Arrow UDTF that partitions its `TABLE` argument by a single column and aggregates results using `terminate`.

#### `test_arrow_udtf_type_coercion_long_to_int`
Verifies that Arrow UDTF output values are automatically coerced from `int64` to `int32` when they fit the target type.

#### `test_arrow_udtf_type_coercion_string_to_int`
Confirms that an error is raised when Arrow UDTF output values (like strings) cannot be coerced to the expected type (like integer).

#### `test_arrow_udtf_type_coercion_string_to_int_safe`
Tests if an Arrow UDTF can safely coerce string values to integer return types.

#### `test_arrow_udtf_type_corecion_int64_to_int32_safe`
Verifies safe type coercion from 64-bit integers to 32-bit integers in Arrow UDTFs.

#### `test_arrow_udtf_with_empty_column_result`
Checks behavior when an Arrow UDTF returns an empty schema or mismatches the expected schema.

#### `test_arrow_udtf_with_empty_table`
Ensures Arrow UDTFs are not called for empty input tables, returning empty results instead.

#### `test_arrow_udtf_with_logging`
Validates that logging within an Arrow UDTF is correctly captured and accessible via 'python_worker_logs'.

#### `test_arrow_udtf_with_named_arguments`
Tests support for named arguments when calling Arrow UDTFs from both DataFrame API and SQL.

#### `test_arrow_udtf_with_named_arguments_scalar_only`
Specifically tests named and positional arguments for Arrow UDTFs with only scalar inputs.

#### `test_arrow_udtf_with_partition_by`
Verifies that 'PARTITION BY' correctly groups data before processing in an Arrow UDTF, using 'terminate' to yield partition sums.

#### `test_arrow_udtf_with_partition_by_and_order_by`
Checks that 'PARTITION BY' and 'ORDER BY' together ensure rows are processed in the correct order within each partition.

#### `test_arrow_udtf_with_partition_by_and_terminate`
Confirms that 'terminate' correctly yields accumulated partition-level statistics (count and sum).

#### `test_arrow_udtf_with_partition_by_empty_input_batch`
Ensures that 'PARTITION BY' on an empty input results in an empty output, correctly skipping 'eval' and 'terminate'.

#### `test_arrow_udtf_with_partition_by_null_values`
Validates handling of null values in both partition keys and input data within a 'PARTITION BY' context.

#### `test_arrow_udtf_with_partition_by_skip_rest_of_input`
Tests the 'SkipRestOfInputTableException' to stop processing a partition early in an Arrow UDTF.

#### `test_arrow_udtf_with_scalar_first_table_second`
Verifies correct handling of arguments when a scalar precedes a table argument in an Arrow UDTF call.

#### `test_arrow_udtf_with_table_argument_and_scalar`
Checks Arrow UDTF calls with a table argument followed by a scalar argument.

#### `test_arrow_udtf_with_table_argument_basic`
Validates the basic functionality of passing a table as an argument to an Arrow UDTF.

#### `test_arrow_udtf_with_table_argument_then_lateral_join_allowed`
Ensures Arrow UDTFs with table arguments can be used in queries that involve joins.

#### `test_arrow_udtf_zero_args`
Tests Arrow UDTFs that take no arguments and return a fixed set of rows.

#### `test_return_type_coercion_multiple_columns`
Verifies simultaneous type coercion for multiple columns in an Arrow UDTF return schema.

#### `test_return_type_coercion_overflow`
Confirms that an exception is raised when Arrow UDTF return values overflow the specified output type.

#### `test_return_type_coercion_success`
Verifies that an Arrow UDTF can successfully coerce a `long` return type to an `int` when specified in the `returnType`.

#### `test_arrow_udtf_with_table_and_struct_arguments`
Test that TABLE args are RecordBatch while struct args are Array.

#### `test_arrow_udtf_with_table_argument_in_middle`
Test Arrow UDTF with table argument in the middle of multiple scalar arguments.

## File: `python/pyspark/sql/tests/connect/client/test_artifact.py`

### Class: `ArtifactTests`

#### `test_add_archive`
Tests adding an archive file to the Spark session and verifies it can be added again from a different session.

#### `test_add_file`
Tests adding a general file to the Spark session and verifies it can be added again from a different session.

#### `test_add_not_existing_artifact`
Ensures that attempting to add a non-existent artifact file raises a `FileNotFoundError`.

#### `test_add_pyfile`
Tests adding a Python file (.py) to the Spark session and verifies it can be added again from a different session.

#### `test_add_zipped_package`
Tests adding a zipped Python package to the Spark session and verifies it can be added again from a different session.

#### `test_artifacts_cannot_be_overwritten`
Confirms that once an artifact is added, attempting to overwrite it with a different file of the same name results in a `DUPLICATED_ARTIFACT` error.

#### `test_basic_requests`
Validates the creation and retrieval of basic artifact requests for a small JAR file.

#### `test_batched_artifacts`
Tests that multiple artifacts can be batched into a single request and verifies their CRC and data integrity.

#### `test_cache_artifact`
Verifies the artifact caching mechanism by checking if an artifact is cached after being added and ensuring the generated hash is correct.

#### `test_chunked_artifacts`
Validates the transfer of large artifacts by splitting them into multiple chunks and verifying each chunk's CRC and data.

#### `test_copy_from_local_to_fs`
Tests the `copyFromLocalToFs` function by copying a local file to a destination path and verifying the content remains unchanged.

#### `test_single_chunk_artifact`
Verifies that a small artifact fits into a single request batch and maintains data integrity.

#### `test_single_chunked_and_chunked_artifact`
Tests a complex scenario where a mix of small (single-batch) and large (multi-chunk) artifacts are requested together.

## File: `python/pyspark/sql/tests/connect/client/test_artifact_localcluster.py`

### Class: `LocalClusterArtifactTests`

#### `test_add_archive`
Similar to `test_add_archive`, but specifically tests adding an archive in a local cluster environment with multiple sessions.

#### `test_add_file`
Similar to `test_add_file`, but specifically tests adding a file in a local cluster environment with multiple sessions.

#### `test_add_pyfile`
Similar to `test_add_pyfile`, but specifically tests adding a Python file in a local cluster environment with multiple sessions.

#### `test_add_zipped_package`
Similar to `test_add_zipped_package`, but specifically tests adding a zipped package in a local cluster environment.

#### `test_artifacts_cannot_be_overwritten`
Verifies that artifacts cannot be overwritten in a local cluster environment, expecting an `ARTIFACT_ALREADY_EXISTS` error.

## File: `python/pyspark/sql/tests/connect/client/test_client.py`

### Class: `SparkConnectClientReattachTestCase`

#### `test_basic_flow`
Tests the basic success path of the ExecutePlanResponseReattachableIterator, verifying the expected number of calls to execute, attach, and release methods on the stub.

#### `test_error_codes`
Validates that various server-side error scenarios (missing status, specific gRPC codes, missing details/metadata, present error class or SQL state) are correctly mapped to SparkConnectGrpcException with the expected properties.

#### `test_fail_and_retry_during_execute`
Checks that the reattachable iterator correctly handles a non-fatal (retriable) error during initial execution by reattaching and continuing.

#### `test_fail_and_retry_during_reattach`
Verifies that the reattachable iterator can handle and retry multiple non-fatal errors during the reattach process itself.

#### `test_fail_during_execute`
Ensures that a fatal (non-retriable) error during execution is correctly raised to the caller by the reattachable iterator.

#### `test_observed_session_id`
Validates that the reattachable iterator correctly propagates the client_observed_server_side_session_id in its reattach requests when provided.

#### `test_server_unreachable`
Verifies that an unreachable server (e.g., DNS failure) results in a retriable UNAVAILABLE gRPC error wrapped in a SparkConnectGrpcException.

#### `test_not_found_fails`
SPARK-48056: Assert that the client fails from session or operation not found error
if a partial response was previously received.

#### `test_not_found_recovers`
SPARK-48056: Assert that the client recovers from session or operation not
found error if no partial responses were previously received.

### Class: `SparkConnectClientTestCase`

#### `test_channel_builder`
Tests a custom `ChannelBuilder` to ensure it correctly propagates a custom `userId` to the `SparkConnectClient`.

#### `test_channel_builder_with_session`
Verifies that the DefaultChannelBuilder correctly extracts and assigns the session_id from the connection string to the SparkConnectClient.

#### `test_custom_operation_id`
Ensures that a custom operation_id provided to _execute_plan_request_with_metadata is correctly propagated in the ExecutePlan response.

#### `test_interrupt_all`
Checks that calling interrupt_all on the client triggers the Interrupt RPC on the server stub.

#### `test_is_closed`
Validates that the is_closed property accurately reflects the client's connection state before and after calling close.

#### `test_on_exit_calls_release_and_close_when_enabled`
Verifies that when _release_session_on_exit is enabled, the client's _on_exit handler correctly calls both release_session and close.

#### `test_properties`
Checks that client properties like token and host are correctly parsed and accessible from the connection string.

#### `test_session_hook`
Tests the custom session hook mechanism, ensuring that hooks are correctly initialized and their on_execute_plan method is called during plan execution and UDF registration.

#### `test_user_agent_default`
Verifies that the client sends a default user agent string following a specific regex pattern in the ExecutePlan request.

#### `test_user_agent_passthrough`
Ensures that a user agent provided in the connection string is correctly prepended to the client's user agent string.

#### `test_user_context_extension`
Comprehensive test for managing user context extensions, verifying that adding, removing, and clearing thread-local and global extensions correctly updates the extensions sent in RPC requests.

#### `test_get_operations_statuses_all`
Test get_operations_statuses returns all operation statuses when no IDs specified.

#### `test_get_operations_statuses_empty`
Test get_operations_statuses returns empty list when no operations exist.

#### `test_get_operations_statuses_specific_ids`
Test get_operations_statuses filters by specific operation IDs.

#### `test_get_operations_statuses_with_operation_extensions`
Test get_operations_statuses passes operation-level extensions and echoes them back per operation.

#### `test_get_operations_statuses_with_request_extensions`
Test _get_operation_statuses sends request-level extensions and echoes them back.

#### `test_on_exit_catches_both_exceptions`
Test _on_exit handles both release_session and close raising exceptions.

#### `test_on_exit_catches_close_exception`
Test _on_exit silently catches exception from close.

#### `test_on_exit_catches_release_session_exception`
Test _on_exit continues to call close even if release_session raises.

#### `test_on_exit_does_not_call_when_already_closed`
Test _on_exit does nothing when client is already closed.

#### `test_on_exit_does_not_call_when_release_disabled`
Test _on_exit does nothing when _release_session_on_exit is False.

## File: `python/pyspark/sql/tests/connect/client/test_client_call_stack_trace.py`

### Class: `CallStackTraceIntegrationTestCase`

#### `test_analyze_plan_request_includes_call_stack`
Test that _analyze_plan_request_with_metadata includes call stack with env var.

#### `test_call_stack_trace_captures_correct_calling_context`
Test that call stack trace captures the correct calling context.

#### `test_config_request_includes_call_stack_with_env_var`
Test that _config_request_with_metadata includes call stack with env var.

#### `test_execute_plan_request_includes_call_stack`
Test that _execute_plan_request_with_metadata includes call stack with env var.

### Class: `CallStackTraceTestCase`

#### `test_build_call_stack_trace_with_env_var_set`
Test that _build_call_stack_trace builds trace when env var is set.

#### `test_build_call_stack_trace_without_env_var`
Test that _build_call_stack_trace returns empty list when env var is not set.

#### `test_is_pyspark_source_with_non_pyspark_file`
Test that _is_pyspark_source correctly identifies non-PySpark files.

#### `test_is_pyspark_source_with_pyspark_file`
Test that _is_pyspark_source correctly identifies PySpark files.

#### `test_retrieve_stack_frames_includes_user_frames`
Test that _retrieve_stack_frames includes user code frames.

## File: `python/pyspark/sql/tests/connect/client/test_client_retries.py`

### Class: `SparkConnectClientRetriesTestCase`

#### `test_default_policy_retries_retry_info`
Checks that the DefaultPolicy correctly retries errors containing RetryInfo even if the gRPC status code itself isn't specifically matched by other policies.

#### `test_max_server_retry_delay`
Validates that the client's retry logic respects the max_server_retry_delay limit when the server-provided retry delay is excessively long.

#### `test_retry`
Verifies that the client's retry mechanism continues to retry a UNAVAILABLE error for a significant duration (at least 10 minutes) as configured by default policies.

#### `test_retry_client_unit`
Verifies that custom retry policies can be set and retrieved correctly on a SparkConnectClient.

#### `test_retry_delay_overrides_max_backoff`
Tests if the retry_delay from a server exception correctly overrides the max_backoff configuration in the client's retry policy.

#### `test_return_to_exponential_backoff`
Ensures that after following a specific retry_delay from the server, the client correctly reverts to its standard exponential backoff strategy for subsequent retries.

#### `test_warning_works`
Validates that a specific warning ([RETRIES_EXCEEDED]) is issued when the maximum number of retries is reached.

## File: `python/pyspark/sql/tests/connect/client/test_reattach.py`

### Class: `SparkConnectReattachTestCase`

#### `test_release_sessions`
Checks that calling release_session on the client effectively cancels active queries and closes the session on the server, resulting in appropriate exceptions for both active and new queries.

## File: `python/pyspark/sql/tests/connect/pandas/streaming/test_parity_pandas_grouped_map_with_state.py`

### Class: `GroupedApplyInPandasWithStateTests`

#### `test_apply_in_pandas_with_state_basic`
Basic test for applyInPandasWithState that updates state with total length of input data and yields a summary row.

#### `test_apply_in_pandas_with_state_basic_fewer_data`
Tests applyInPandasWithState when the user function yields fewer rows than the number of keys.

#### `test_apply_in_pandas_with_state_basic_more_data`
Tests applyInPandasWithState when the user function yields multiple rows per key.

#### `test_apply_in_pandas_with_state_basic_no_state`
Verifies applyInPandasWithState behavior when no state updates are performed but data is returned.

#### `test_apply_in_pandas_with_state_basic_no_state_no_data`
Tests applyInPandasWithState when neither state is updated nor data is returned.

#### `test_apply_in_pandas_with_state_basic_with_null`
Validates applyInPandasWithState correctly handles null values in the output keys.

#### `test_apply_in_pandas_with_state_int_to_decimal_coercion`
Tests the automatic coercion of integer results to decimal types in applyInPandasWithState when the configuration is enabled.

#### `test_apply_in_pandas_with_state_python_worker_random_failure`
A resilience test that ensures applyInPandasWithState queries can recover and produce correct results even if the Python worker process fails randomly.

## File: `python/pyspark/sql/tests/connect/pandas/streaming/test_parity_pandas_transform_with_state.py`

### Class: `TransformWithStateInPandasParityTests`

#### `test_composite_output_schema`
Validates transformWithStateInPandas with a complex, nested output schema containing arrays, maps, and structs.

#### `test_not_nullable_fails`
Ensures that transformWithState correctly throws an error if the state schema is not nullable when using Avro encoding.

#### `test_schema_evolution_fails`
Tests that invalid state schema evolution (e.g., changing field types from Long to Int) results in a STATE_STORE_INVALID_VALUE_SCHEMA_EVOLUTION error.

#### `test_schema_evolution_scenarios`
A placeholder test intended for testing various schema evolution scenarios.

#### `test_transform_with_state_basic`
Basic functional test for transformWithStateInPandas comparing batch results across different trigger batches.

#### `test_transform_with_state_batch_query`
Tests transformWithStateInPandas in a batch (non-streaming) query context.

#### `test_transform_with_state_batch_query_initial_state`
Verifies that transformWithStateInPandas correctly incorporates initial state data in a batch query.

#### `test_transform_with_state_chaining_ops`
Tests chaining operations with `transformWithStateInPandas` in a streaming query, verifying that watermarks and late event dropping work correctly across multiple batches of data.

#### `test_transform_with_state_event_time`
Tests event time based stateful processing with timers, verifying that timers registered with specific expiration times are correctly handled and expired at the expected batch based on event time watermarks.

#### `test_transform_with_state_in_pandas_composite_type`
Tests `transformWithStateInPandas` with composite types like arrays and maps stored in state, verifying that list and map states handle complex data structures correctly.

#### `test_transform_with_state_int_to_decimal_coercion`
Tests automatic type coercion from integer to decimal in `transformWithStateInPandas`, verifying that enabling `spark.sql.execution.pythonUDF.pandas.intToDecimalCoercionEnabled` allows successful conversion while disabling it raises an exception.

#### `test_transform_with_state_large_values`
Tests handling of large values (e.g., 512 KB strings) in value state, list state, and map state within `transformWithStateInPandas`.

#### `test_transform_with_state_non_contiguous_grouping_cols`
Tests `transformWithStateInPandas` when the grouping columns are not contiguous in the input DataFrame, ensuring correct key identification.

#### `test_transform_with_state_non_contiguous_grouping_cols_with_init_state`
Tests `transformWithStateInPandas` with non-contiguous grouping columns in both input and initial state DataFrames, verifying correct state loading.

#### `test_transform_with_state_proc_timer`
Tests processing time based timers in `transformWithStateInPandas`, verifying that timers expire based on processing time and state is updated accordingly.

#### `test_transform_with_state_query_restarts`
Tests that stateful queries using `transformWithStateInPandas` can be stopped and restarted from a checkpoint correctly, preserving custom metrics.

#### `test_transform_with_state_with_bytes_limit`
Tests the effect of the `spark.sql.execution.arrow.maxBytesPerBatch` configuration on `transformWithStateInPandas`, verifying data chunking behavior.

#### `test_transform_with_state_with_records_limit`
Tests the effect of `spark.sql.execution.arrow.maxRecordsPerBatch` configuration on `transformWithStateInPandas`, verifying data chunking based on record count.

#### `test_transform_with_state_with_timers_single_partition`
A wrapper test that runs event time and processing time timer tests with a single shuffle partition to ensure correct behavior.

#### `test_transform_with_state_with_wmark_and_non_event_time`
Tests `transformWithStateInPandas` with event time processing but without event time output mode, verifying watermark behavior.

## File: `python/pyspark/sql/tests/connect/pandas/streaming/test_parity_pandas_transform_with_state_state_variable.py`

### Class: `TransformWithStateInPandasStateVariableParityTests`

#### `test_transform_with_list_state_metadata`
Tests reading list state metadata and data using the state metadata and state store data sources, verifying operator properties and stored data.

#### `test_transform_with_map_state_metadata`
A wrapper that calls a helper method to test map state metadata, likely similar to the list state metadata test.

#### `test_transform_with_map_state_metadata_with_init_state`
Tests map state metadata similar to the previous test, but explicitly provides a no-op initial state DataFrame.

#### `test_transform_with_state_basic`
A basic test for `transformWithStateInPandas` that verifies simple counting logic based on value state across two batches.

#### `test_transform_with_state_init_state`
Tests that initial state is correctly processed only once in the first batch and subsequent batches accumulate state starting from the initial values.

#### `test_transform_with_state_init_state_with_extra_transformation`
Tests initial state handling with an extra non-stateful transformation applied after `transformWithStateInPandas`.

#### `test_transform_with_state_init_state_with_timers`
Tests that timers registered during initial state processing are correctly triggered and handled, even for keys not in the first batch of regular input.

#### `test_transform_with_state_list_state`
Tests basic functionality of transformWithState with list state, verifying that counts are correctly accumulated and returned for multiple keys.

#### `test_transform_with_state_list_state_large_list`
Verifies that list state can handle a large number of elements across multiple batches in a streaming query.

#### `test_transform_with_state_list_state_large_ttl`
Tests transformWithState with list state using a large TTL (Time-To-Live) configuration.

#### `test_transform_with_state_map_state`
Tests basic functionality of transformWithState using map state.

#### `test_transform_with_state_map_state_large_ttl`
Tests transformWithState with map state using a large TTL configuration.

#### `test_transform_with_state_non_exist_value_state`
Verifies behavior when accessing a non-existent value state, expecting a count of 0.

#### `test_transform_with_state_restart_with_multiple_rows_init_state`
Tests loading a streaming query with initial state derived from a previous query's checkpoint, where multiple rows exist for the same grouping key.

#### `test_transform_with_state_with_timers_single_partition`
Runs event time and processing time timer tests for transformWithState under a single shuffle partition configuration.

#### `test_transform_with_value_state_metadata`
Verifies state metadata source reading and change feed capabilities (reading state updates between batches) for value state in a streaming query.

#### `test_value_state_ttl_basic`
Tests basic Time-To-Live (TTL) functionality for value state, checking state values across two batches.

#### `test_value_state_ttl_expiration`
Verifies that states correctly expire according to TTL rules and that updating state resets its TTL.

## File: `python/pyspark/sql/tests/connect/pandas/streaming/test_parity_transform_with_state.py`

### Class: `TransformWithStateInPySparkParityTests`

#### `test_composite_output_schema`
Validates that transformWithState correctly produces output matching a complex schema involving arrays, maps, and nested structs.

#### `test_not_nullable_fails`
Confirms that an error is thrown if a state schema used with Avro encoding has non-nullable columns.

#### `test_schema_evolution_fails`
Tests that schema evolution fails when trying to use an incompatible schema (Int) after a chain of valid evolutions in Avro encoding.

#### `test_schema_evolution_scenarios`
Placeholder for testing schema evolution scenarios.

#### `test_transform_with_state_basic`
Tests basic state updates and results for multiple keys across streaming batches.

#### `test_transform_with_state_batch_query`
Validates that transformWithState operations function correctly on static (batch) DataFrames.

#### `test_transform_with_state_batch_query_initial_state`
Validates that transformWithState with initial state functions correctly on static (batch) DataFrames.

#### `test_transform_with_state_chaining_ops`
Tests advanced scenarios chaining transformWithState operations with specific event-time watermark and eviction semantics.

#### `test_transform_with_state_event_time`
Validates handling of event time watermarks and timers in transformWithState operations.

#### `test_transform_with_state_in_pandas_composite_type`
Tests transformWithStateInPandas with composite types (maps and nested maps) in the state, verifying that state is correctly updated and read across batches.

#### `test_transform_with_state_int_to_decimal_coercion`
Verifies that integer values in Pandas DataFrames are correctly coerced to decimal types in the output schema when the configuration spark.sql.execution.pythonUDF.pandas.intToDecimalCoercionEnabled is enabled, and fails appropriately when disabled.

#### `test_transform_with_state_large_values`
Tests the handling of large values (approximately 512 KB) within value, list, and map states during arbitrary stateful processing, ensuring data integrity for large payloads.

#### `test_transform_with_state_non_contiguous_grouping_cols`
Tests that transformWithState works correctly when the grouping columns are not contiguous in the input DataFrame schema.

#### `test_transform_with_state_non_contiguous_grouping_cols_with_init_state`
Tests transformWithState with non-contiguous grouping columns in both the input data and the initial state DataFrame, verifying correct state initialization and processing.

#### `test_transform_with_state_proc_timer`
Verifies the functionality of processing time timers in stateful processing, ensuring that timers expire correctly and that timestamps are consistent with processing order.

#### `test_transform_with_state_query_restarts`
Tests the resilience and correctness of transformWithState across query restarts, ensuring state is preserved and restored correctly from the checkpoint.

#### `test_transform_with_state_with_bytes_limit`
Verifies that stateful processing respects the Arrow batch size limit in bytes, correctly splitting processing into multiple chunks when the limit is low.

#### `test_transform_with_state_with_records_limit`
Verifies that stateful processing respects the Arrow batch size limit in records, correctly splitting processing into multiple chunks when the limit is low.

#### `test_transform_with_state_with_timers_single_partition`
Runs event time and processing time timer tests with a single shuffle partition to ensure correct behavior in a simplified execution environment.

#### `test_transform_with_state_with_wmark_and_non_event_time`
Tests stateful processing behavior when combined with watermarks but using non-event-time modes (ProcessingTime or None).

## File: `python/pyspark/sql/tests/connect/pandas/streaming/test_parity_transform_with_state_state_variable.py`

### Class: `TransformWithStateInPySparkStateVariableParityTests`

#### `test_transform_with_list_state_metadata`
Verifies that metadata and actual data for list state can be correctly queried using the state store and state-metadata data sources, including support for flattening collection types.

#### `test_transform_with_map_state_metadata`
Verifies that metadata for map state can be correctly queried without initial state.

#### `test_transform_with_map_state_metadata_with_init_state`
Verifies that metadata for map state can be correctly queried when the query is executed with an initial state.

#### `test_transform_with_state_basic`
A basic test for transformWithState to ensure that standard state operations (accumulating counts) produce expected results across multiple batches.

#### `test_transform_with_state_init_state`
Tests that transformWithState correctly applies initial state data to the stateful processor during query startup.

#### `test_transform_with_state_init_state_with_extra_transformation`
Tests transformWithState with initial state and an additional downstream transformation, ensuring that data passes correctly through the pipeline.

#### `test_transform_with_state_init_state_with_timers`
Tests that initial state combined with registered timers correctly processes expired timers on query startup.

#### `test_transform_with_state_list_state`
Basic test for list state in arbitrary stateful processing, asserting expected counts for each group.

#### `test_transform_with_state_list_state_large_list`
Tests that list state can handle a large number of elements across batches without performance or data loss issues.

#### `test_transform_with_state_list_state_large_ttl`
Verifies transformWithState using list state with a large Time-To-Live (TTL) configuration.

#### `test_transform_with_state_map_state`
Verifies transformWithState using map state.

#### `test_transform_with_state_map_state_large_ttl`
Verifies transformWithState using map state with a large Time-To-Live (TTL) configuration.

#### `test_transform_with_state_non_exist_value_state`
Verifies transformWithState behavior when accessing non-existent value state.

#### `test_transform_with_state_restart_with_multiple_rows_init_state`
Verifies transformWithState query restart with initial state containing multiple rows per key.

#### `test_transform_with_state_with_timers_single_partition`
Runs event time and processing time timer tests for transformWithState with a single shuffle partition.

#### `test_transform_with_value_state_metadata`
Verifies state metadata and state data source reading from checkpoints.

#### `test_value_state_ttl_basic`
Verifies basic Time-To-Live (TTL) functionality for value state in transformWithState.

#### `test_value_state_ttl_expiration`
Verifies that state correctly expires after its Time-To-Live (TTL) duration in transformWithState.

## File: `python/pyspark/sql/tests/connect/pandas/test_parity_pandas_cogrouped_map.py`

### Class: `CogroupedApplyInPandasTests`

#### `test_apply_in_pandas_not_returning_pandas_dataframe`
Verifies handling when applyInPandas UDF does not return a pandas DataFrame.

#### `test_apply_in_pandas_returning_column_names`
Verifies applyInPandas when the returned pandas DataFrame includes column names.

#### `test_apply_in_pandas_returning_column_names_sometimes`
Verifies applyInPandas when the returned pandas DataFrame only sometimes includes column names.

#### `test_apply_in_pandas_returning_empty_dataframe`
Verifies applyInPandas when the UDF returns an empty pandas DataFrame for some groups.

#### `test_apply_in_pandas_returning_incompatible_type`
Verifies handling when applyInPandas UDF returns data types incompatible with the specified schema.

#### `test_apply_in_pandas_returning_no_column_names`
Verifies applyInPandas when the returned pandas DataFrame does not include column names.

#### `test_apply_in_pandas_returning_no_column_names_and_wrong_amount`
Verifies handling when returned pandas DataFrame has no column names and wrong column count.

#### `test_apply_in_pandas_returning_wrong_column_names`
Verifies handling when returned pandas DataFrame has column names that do not match the schema.

#### `test_arrow_batch_slicing`
Verifies that cogroup().applyInPandas() correctly handles Arrow batch slicing based on record and byte limits.

#### `test_case_insensitive_grouping_column`
Verifies that grouping columns in cogroup().applyInPandas() are case-insensitive.

#### `test_cogroup_apply_in_pandas_with_logging`
Verifies that logs from pandas UDF in cogroup().applyInPandas() are correctly captured.

#### `test_cogroup_apply_int_to_decimal_coercion`
Verifies that integer to decimal coercion in applyInPandas works when enabled and fails when disabled.

#### `test_complex_group_by`
Tests co-grouping and applying a pandas function using a complex group-by expression.

#### `test_different_group_key_cardinality`
Verifies behavior when co-grouped DataFrames have different group key cardinalities.

#### `test_different_keys`
Tests co-grouping DataFrames using different column names as keys.

#### `test_different_schemas`
Tests co-grouping DataFrames with different schemas.

#### `test_empty_group_by`
Tests co-grouping without specifying any group-by keys.

#### `test_left_group_empty`
Tests co-grouping when some groups in the left DataFrame are empty.

#### `test_mixed_scalar_udfs_followed_by_cogrouby_apply`
Tests applying mixed scalar UDFs followed by a co-grouped applyInPandas operation.

#### `test_negative_and_zero_batch_size`
Tests co-grouped applyInPandas with negative and zero Arrow batch sizes.

#### `test_right_group_empty`
Tests co-grouping when some groups in the right DataFrame are empty.

#### `test_self_join`
Tests a self-join operation on a DataFrame with a filtered version of itself.

#### `test_simple`
Tests the retry behavior of the Spark Connect client when encountering gRPC internal errors.

#### `test_with_key_complex`
Tests co-grouping with a complex key and accessing the key within the applied pandas function.

#### `test_with_key_left`
Tests co-grouping and accessing the group key in the applied function, focusing on the left DataFrame.

#### `test_with_key_left_group_empty`
Tests co-grouping and accessing the group key in the applied function, with some left groups empty.

#### `test_with_key_right`
Tests co-grouping and accessing the group key in the applied function, focusing on the right DataFrame.

#### `test_with_key_right_group_empty`
Tests co-grouping and accessing the group key in the applied function, with some right groups empty.

#### `test_with_window_function`
Tests co-grouping operation combined with window functions.

#### `test_wrong_args`
Verifies error handling when wrong arguments are passed to co-grouped applyInPandas.

#### `test_wrong_return_type`
Verifies error handling when the applied function in co-grouped applyInPandas returns an invalid type.

## File: `python/pyspark/sql/tests/connect/pandas/test_parity_pandas_grouped_map.py`

### Class: `ApplyInPandasTests`

#### `test_apply_in_pandas_int_to_decimal_coercion`
Verifies that integer to decimal coercion in `applyInPandas` works when the configuration `spark.sql.execution.pythonUDF.pandas.intToDecimalCoercionEnabled` is set to True, and raises a PythonException when set to False.

#### `test_apply_in_pandas_iterator_basic`
Tests basic usage of `applyInPandas` with an iterator UDF, verifying that it correctly computes the sum of values per group.

#### `test_apply_in_pandas_iterator_batch_slicing`
Tests `applyInPandas` with an iterator across different Arrow batch configurations (`maxRecordsPerBatch` and `maxBytesPerBatch`) to verify correct min/max aggregation despite batch slicing.

#### `test_apply_in_pandas_iterator_filter_multiple_batches`
Tests `applyInPandas` with an iterator that processes multiple input batches, filters even and odd values separately, and yields them in separate batches, verifying all rows are preserved.

#### `test_apply_in_pandas_iterator_multiple_output_batches`
Tests `applyInPandas` with an iterator that yields multiple output batches (one per row) for each input batch, verifying correct reconstruction of the dataset.

#### `test_apply_in_pandas_iterator_partial_iteration`
Tests `applyInPandas` with an iterator that only consumes the first batch from the input iterator, verifying expected partial results.

#### `test_apply_in_pandas_iterator_process_multiple_input_batches`
Tests `applyInPandas` with an iterator processing large datasets with small batch sizes, verifying that multiple batches are processed per group and the total sum is correct.

#### `test_apply_in_pandas_iterator_streaming_aggregation`
Tests `applyInPandas` with an iterator performing a streaming-like aggregation, maintaining running counts and averages across batches and yielding intermediate results.

#### `test_apply_in_pandas_iterator_with_compression_codec`
Tests `applyInPandas` with an iterator UDF across various Arrow compression codecs ('none', 'zstd', 'lz4') to ensure correctness.

#### `test_apply_in_pandas_iterator_with_keys`
Tests `applyInPandas` where the UDF takes both the grouping key and an iterator of batches, verifying correct access to key values.

#### `test_apply_in_pandas_iterator_with_keys_batch_slicing`
Tests `applyInPandas` with keys and batch slicing, verifying min/max aggregation across multiple batches per group.

#### `test_apply_in_pandas_iterator_with_keys_multiple_batches`
Tests `applyInPandas` with keys, where the UDF splits batches and yields multiple output batches including the key, verifying all data is present.

#### `test_apply_in_pandas_not_returning_pandas_dataframe`
Verifies that `applyInPandas` fails appropriately when the UDF does not return a pandas DataFrame.

#### `test_apply_in_pandas_returning_column_names`
Tests `applyInPandas` where the UDF returns a DataFrame with labeled column names (from a merge operation).

#### `test_apply_in_pandas_returning_column_names_sometimes`
Tests `applyInPandas` where the UDF conditionally returns column names or positional columns, verifying correct handling.

#### `test_apply_in_pandas_returning_empty_dataframe`
Tests `applyInPandas` where the UDF may return an empty DataFrame for certain groups, verifying correct execution.

#### `test_apply_in_pandas_returning_incompatible_type`
Verifies that `applyInPandas` fails when the UDF returns a DataFrame with types incompatible with the requested schema.

#### `test_apply_in_pandas_returning_no_column_names`
Tests `applyInPandas` where the UDF returns a DataFrame without column names (positional columns), verifying correct mapping to schema.

#### `test_apply_in_pandas_returning_no_column_names_and_wrong_amount`
Verifies failure when UDF returns no column names and a different number of columns than the schema expects.

#### `test_apply_in_pandas_returning_wrong_column_names`
Verifies failure when UDF returns column names that do not match the expected schema.

#### `test_apply_in_pandas_with_compression_codec`
Tests `applyInPandas` with different Arrow compression codecs ('none', 'zstd', 'lz4') to ensure correct results.

#### `test_apply_in_pandas_with_logging`
Tests that logging within a function passed to `applyInPandas` works correctly and can be retrieved using `python_worker_logs`.

#### `test_array_type_correct`
Verifies that `GROUPED_MAP` Pandas UDF correctly handles `ArrayType` in the output schema.

#### `test_arrow_batch_slicing`
Tests `cogroup.applyInPandas` with different `maxRecordsPerBatch` and `maxBytesPerBatch` configurations to verify Arrow batch slicing behavior.

#### `test_arrow_cast_enabled_numeric_to_decimal`
Tests that numeric types (int, float) in Pandas can be correctly cast to `DecimalType` when returned from a scalar Pandas UDF.

#### `test_arrow_cast_enabled_str_to_numeric`
Tests that string types in Pandas can be correctly cast to numeric types (Integer, Long, Float, Double) when returned from a scalar Pandas UDF.

#### `test_case_insensitive_grouping_column`
Verifies that grouping columns are case-insensitive when using `cogroup.applyInPandas`.

#### `test_coerce`
Tests that the result of a `GROUPED_MAP` Pandas UDF is coerced to the specified schema types (e.g., int to double).

#### `test_column_order`
Suppresses output and calls `check_column_order`.

#### `test_complex_groupby`
Tests `groupBy` with a complex expression (column modulo 2) followed by `apply` with a Pandas UDF.

#### `test_datatype_string`
Tests passing a string schema to `pandas_udf` with `GROUPED_MAP` and verifies results.

#### `test_decorator`
Tests using the `@pandas_udf` decorator for `GROUPED_MAP` and verifies results.

#### `test_empty_groupby`
Tests applying a `GROUPED_MAP` Pandas UDF on an empty group-by (grouping by nothing).

#### `test_grouped_map_pandas_udf_with_compression_codec`
Tests `@pandas_udf` with `GROUPED_MAP` under different Arrow compression codec settings.

#### `test_grouped_over_window`
Tests `groupby` with a window expression followed by `applyInPandas`.

#### `test_grouped_over_window_with_key`
Tests `groupby` with a window expression followed by `applyInPandas` where the function takes a key argument, verifying the key contains correct group and window values.

#### `test_grouped_with_empty_partition`
Tests that `GROUPED_MAP` works correctly even when some partitions are empty.

#### `test_mixed_scalar_udfs_followed_by_groupby_apply`
Tests a pipeline with mixed regular UDF and scalar Pandas UDF followed by a group-by and apply with another Pandas UDF.

#### `test_negative_and_zero_batch_size`
Tests that zero and negative values for `spark.sql.execution.arrow.maxRecordsPerBatch` are handled (by calling `test_with_key_right`).

#### `test_positional_assignment_conf`
Tests positional assignment of columns (instead of by name) in `GROUPED_MAP` by setting `spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName` to false.

#### `test_register_grouped_map_udf`
Verifies that registering a grouped map UDF in the catalog fails with an appropriate error, as it's not a supported UDF type for catalog registration.

#### `test_self_join_with_pandas`
Tests that a self-join on a DataFrame that was created by applying a grouped map UDF works correctly and produces the expected number of rows.

#### `test_supported_types`
Validates that grouped map UDFs correctly handle a wide variety of Spark data types, comparing Spark results with direct Pandas operations.

#### `test_timestamp_dst`
Verifies that scalar and scalar iterator UDFs handle timestamps correctly across Daylight Saving Time transitions without altering the values.

#### `test_udf_with_key`
Tests grouped map UDFs that accept grouping keys as arguments, covering various grouping scenarios (single column, expression, multiple keys, and no keys) and validating results.

#### `test_unsupported_types`
Helper test that checks behavior when unsupported types are used in the context of grouped map operations.

#### `test_wrong_args`
Helper test that checks behavior when incorrect arguments are passed to cogrouped apply in pandas operations.

#### `test_wrong_args_in_apply_func`
Verifies that passing a function with an invalid number of arguments to applyInPandas or applyInArrow (both grouped and cogrouped) raises a proper error.

#### `test_wrong_return_type`
Helper test that checks behavior when a UDF returns a type that does not match the specified return schema.

## File: `python/pyspark/sql/tests/connect/pandas/test_parity_pandas_map.py`

### Class: `MapInPandasParityTests`

#### `test_chain_map_partitions_in_pandas`
Tests that multiple mapInPandas transformations can be successfully chained together and produce correct results.

#### `test_dataframes_with_duplicate_column_names`
Helper test that checks the behavior of mapInPandas when the resulting Pandas DataFrames have duplicate column names.

#### `test_dataframes_with_incompatible_types`
Helper test that checks behavior when Pandas DataFrames returned by the UDF have types incompatible with specified Spark schema.

#### `test_dataframes_with_less_columns`
Helper test that checks behavior when Pandas DataFrames returned by the UDF have fewer columns than specified in the Spark schema.

#### `test_dataframes_with_more_columns`
Tests that if the UDF in mapInPandas returns a DataFrame with more columns than the specified Spark schema, the extra columns are correctly ignored.

#### `test_dataframes_with_other_column_names`
Helper test that checks behavior when Pandas DataFrames returned by the UDF have different column names than specified in the Spark schema.

#### `test_different_output_length`
Verifies that mapInPandas can produce a different number of rows than the input partition size.

#### `test_empty_dataframes`
Tests that mapInPandas correctly handles cases where the UDF returns an empty Pandas DataFrame, resulting in an empty Spark DataFrame.

#### `test_empty_dataframes_with_less_columns`
Helper test that checks behavior when UDF returns an empty DataFrame with fewer columns than expected.

#### `test_empty_dataframes_with_more_columns`
Verifies that mapInPandas correctly handles a mix of non-empty and empty Pandas DataFrames with extra columns, and retains correct row count.

#### `test_empty_dataframes_with_other_columns`
Helper test that checks behavior when UDF returns an empty DataFrame with different column names.

#### `test_empty_dataframes_without_columns`
Verifies that mapInPandas correctly handles empty dataframes without columns from the iterator, asserting the total count.

#### `test_empty_iterator`
Verifies that mapInPandas returns an empty result (count 0) when the UDF iterator returns no data.

#### `test_large_variable_types`
Verifies that mapInPandas works correctly when Arrow large variable types are enabled for string and binary data.

#### `test_map_in_pandas`
Verifies that mapInPandas works correctly when returning both an iterator and a list of pandas DataFrames.

#### `test_map_in_pandas_top_level_wrong_order`
Verifies that mapInPandas correctly handles pandas DataFrames with columns in a different order than specified in the Spark schema.

#### `test_map_in_pandas_type_mismatch`
Verifies that mapInPandas raises a PySparkValueError when the pandas DataFrame data type cannot be converted to the specified Arrow type.

#### `test_map_in_pandas_with_barrier_mode`
Verifies that mapInPandas correctly sets up barrier mode execution and makes BarrierTaskContext available when requested.

#### `test_map_in_pandas_with_column_vector`
Verifies that mapInPandas works correctly when off-heap column vectors are enabled/disabled while reading Parquet data.

#### `test_map_in_pandas_with_logging`
Verifies that logging from the pandas worker is correctly captured and accessible via the python_worker_logs function.

#### `test_multiple_columns`
Verifies that mapInPandas correctly handles multiple columns and preserves their types.

#### `test_no_column_names`
Verifies that mapInPandas works correctly even if the pandas DataFrame columns are renamed to integer indices.

#### `test_not_null`
Verifies that mapInPandas works correctly with a schema that specifies non-nullable columns when the data is valid.

#### `test_other_than_dataframe_iter`
Verifies behavior when the iterator returns something other than a dataframe (delegated to check_other_than_dataframe_iter).

#### `test_self_join`
Verifies a specific self-join behavior in Spark Connect that was failing in classic Spark.

#### `test_violate_not_null`
Verifies that mapInPandas raises an exception when the data violates a 'not null' constraint in the schema.

## File: `python/pyspark/sql/tests/connect/pandas/test_parity_pandas_udf.py`

### Class: `PandasUDFParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_pandas_udf_arrow_overflow`
This test verifies the behavior of Arrow when converting pandas UDF results that might cause an overflow (e.g., trying to fit 128 into a `byte` type which ranges from -128 to 127). It checks that an exception is raised when safe type checking is enabled, and that the operation proceeds when it is disabled.

#### `test_pandas_udf_basic`
This test checks basic properties (return type and evaluation type) of pandas UDFs created with different syntaxes and configurations (e.g., specifying `DoubleType` and `VariantType`, and different UDF types like `SCALAR` and `GROUPED_MAP`).

#### `test_pandas_udf_basic_with_return_type_string`
This test is similar to the basic one, but it verifies that pandas UDFs can be created successfully by specifying return types as string representations (e.g., `"double"`, `"variant"`, `"v double"`) instead of instances of Spark types.

#### `test_pandas_udf_day_time_interval_type`
This test checks the support for `DayTimeIntervalType` in pandas UDFs. It verifies that a timedelta value passed into a UDF is processed correctly and can be retrieved from the resulting DataFrame.

#### `test_pandas_udf_decorator`
This test verifies that the `@pandas_udf` decorator works correctly when applied to function definitions, with various ways of specifying return type and function type.

#### `test_pandas_udf_decorator_with_return_type_string`
This test verifies that the `@pandas_udf` decorator works correctly when applied to function definitions, with return types specified as strings.

#### `test_pandas_udf_detect_unsafe_type_conversion`
This test checks that Arrow raises an error when a pandas UDF attempts an unsafe type conversion (e.g., float to integer) and safe type conversion checks are enabled. It also confirms that the conversion goes through when the check is disabled.

#### `test_pandas_udf_empty_frame`
This test ensures that executing a scalar pandas UDF on an empty DataFrame doesn't throw an error and results in an empty result set.

#### `test_pandas_udf_int_to_decimal_coercion`
This test checks the automatic type coercion behavior from integer to decimal in pandas UDFs, verifying both expected success and failure cases regulated by the configuration flag `spark.sql.execution.pythonUDF.pandas.intToDecimalCoercionEnabled`.

#### `test_pandas_udf_nullable_large_integers`
This test verifies that a pandas UDF properly handles a series of large nullable integers without precision loss, specifically testing when the `preferIntExtensionDtype` configuration is enabled.

#### `test_pandas_udf_return_type_error`
This test ensures that Spark raises a proper `PythonException` when a pandas UDF returns a result with an invalid schema that does not match the return type specified.

#### `test_pandas_udf_timestamp_ntz`
This test validates that `TimestampNTZType` (Timestamp No Timezone) can be handled correctly in a pandas UDF without automatic adjustment to local timezone.

#### `test_stopiteration_in_grouped_agg`
This test verifies that `StopIteration` exceptions raised from inside a pandas grouped aggregate UDF are handled properly and translated to a `PythonException` on the Spark driver.

#### `test_stopiteration_in_grouped_map`
This test checks that a `StopIteration` exception in a pandas grouped map UDF raises a `PythonException` with the correct error message on the Spark driver.

#### `test_stopiteration_in_udf`
Similar to previous tests, this one checks that a `StopIteration` exception raised inside a plain scalar UDF or a pandas scalar UDF is properly bubbled up as a `PythonException`.

#### `test_udf_wrong_arg`
This test validates that pandas UDFs correctly reject invalid return types, invalid schema strings, and mismatched function signatures during UDF registration.

## File: `python/pyspark/sql/tests/connect/pandas/test_parity_pandas_udf_grouped_agg.py`

### Class: `PandasUDFGroupedAggParityTests`

#### `test_0_args`
This test makes sure that Python scalar UDFs with no input arguments work as expected by verifying that they create batched evaluation plans.

#### `test_alias`
Testing supported and unsupported alias

#### `test_array_type`
This test verifies that a pandas grouped aggregate UDF can return values of type `ArrayType[DoubleType]`.

#### `test_arrow_batch_slicing`
This test verifies the correct behavior of the `applyInPandas` operation on cogrouped DataFrames across different batch size and boundary settings, specifically checking whether records are sliced correctly when the number of records or bytes per batch is constrained by configuration.

#### `test_arrow_cast_enabled_numeric_to_decimal`
This test confirms that Arrow is able to correctly cast different numeric types (e.g., int8, int16, uint16, float64) from pandas objects to Spark's `DecimalType` when safe casting is permitted.

#### `test_arrow_cast_enabled_str_to_numeric`
This test checks if Arrow can successfully perform type casts from string to various numeric Spark types inside pandas UDFs without errors.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_basic`
This test verifies basic functionalities of pandas group aggregate UDFs combined with regular expressions and columns. It compares pandas UDF results with results calculated by native Spark standard functions.

#### `test_complex_expressions`
This test verifies complex evaluation paths by chaining multiple types of functions and expressions including native Spark functions, Python scalar UDFs, and pandas group aggregate UDFs, checking that they all play well together.

#### `test_complex_groupby`
This test verifies that `applyInPandas` (or `groupBy.apply`) behaves correctly when the grouping is done on a complex expression (in this case, checking if the ID is even), and not just a single column.

#### `test_grouped_agg_pandas_udf_with_compression_codec`
This test makes sure that grouped aggregate pandas UDFs work fine across multiple Arrow compression settings such as zstd, lz4 and no compression.

#### `test_grouped_agg_pandas_udf_with_compression_codec_complex`
This test ensures that running multiple pandas grouped aggregate UDFs at once on DataFrames functions properly when Arrow compression is active in the environment.

#### `test_grouped_agg_pandas_udf_with_logging`
This test asserts that logs recorded inside a pandas grouped aggregate UDF are correctly captured and can be queried later from the execution environment using `tvf.python_worker_logs`.

#### `test_grouped_agg_with_struct_type_input`
Test that grouped agg UDF works with struct type input.
Struct types should be passed as pd.DataFrame to the UDF (similar to scalar pandas UDFs).

#### `test_grouped_with_empty_partition`
This test ensures that grouped map pandas UDFs work correctly when executed against a dataset partitioned in a way that generates empty partitions, proving the system scales appropriately.

#### `test_grouped_without_group_by_clause`
This test checks that grouped aggregate pandas UDFs can be executed in the absence of a `groupBy` clause, basically computing summary statistics over the entire DataFrame.

#### `test_invalid_args`
This test checks edge cases and negative cases regarding wrong inputs provided to pandas UDF creation APIs, handled silently to prevent screen pollution.

#### `test_iterator_grouped_agg_basic`
Test basic functionality of iterator grouped agg pandas UDF with Iterator[pd.Series].

#### `test_iterator_grouped_agg_eval_type`
Test that the eval type is correctly inferred for iterator grouped agg UDFs.

#### `test_iterator_grouped_agg_multiple_columns`
Test iterator grouped agg pandas UDF with multiple columns
using Iterator[Tuple[pd.Series, ...]].

#### `test_iterator_grouped_agg_partial_consumption`
Test that iterator grouped agg UDF can partially consume batches.
This ensures that batches are processed one by one without loading all data into memory.

#### `test_iterator_grouped_agg_with_struct_type_input`
Test that iterator grouped agg UDF works with struct type input.
Struct types should be passed as pd.DataFrame to the UDF (similar to scalar pandas UDFs).

#### `test_kwargs`
This test checks whether Python UDF calls can map and fill named/keyword arguments correctly, validating correct parameter mapping and negative cases.

#### `test_manual`
This test validates that multiple pandas grouped aggregate UDFs output the exact same values against established, hardcoded test expectation matrices.

#### `test_mixed_sql`
This test checks support for running pandas grouped aggregate UDFs evaluated over a window, and confirms results match results from Spark's native function execution on identical windows.

#### `test_mixed_udfs`
Test mixing group aggregate pandas UDF with python UDF and scalar pandas UDF.

#### `test_multiple_udfs`
This test validates registering multiple UDFs in a row using standard catalogs, then queries DataFrames with expressions that chain them together in SQL strings.

#### `test_named_arguments`
This test validates keyword arguments used as parameter mapping mechanics when executing UDFs on DataFrames or within SQL blocks.

#### `test_named_arguments_and_defaults`
This test examines scenarios where default parameters might are passed on UDF function creation, making sure they work seamlessly across both Dataset and SQL code paths.

#### `test_named_arguments_negative`
This test verifies that duplicate parameter assignments, non-existing keyword arguments or positional argument misplacements are captured correctly for UDF execution.

#### `test_no_predicate_pushdown_through`
This test evaluates logical plan optimization, ensuring that filter predicates referencing results produced by a python grouped aggregate UDF are not pushed down, avoiding broken logical execution flows.

#### `test_register_grouped_agg_iter_udf`
Test registering a grouped aggregate iterator UDF for SQL usage.

#### `test_register_vectorized_udf_basic`
This test checks basic pandas UDF functionality when registered using Catalogs, comparing collection results computed after evaluation paths have resolved against non-UDF baseline paths.

#### `test_retain_group_columns`
This test validates DataFrame behavior when the configuration to drop columns that have been resolved as part of `groupBy` clauses is active.

#### `test_unsupported_types`
This test verifies that pandas UDFs throw expected exceptions when passed data types that they are not designed to support.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/pandas/test_parity_pandas_udf_scalar.py`

### Class: `PandasUDFScalarParityTests`

#### `test_arrow_cast_enabled_numeric_to_decimal`
This test confirms that Arrow is able to correctly cast different numeric types (e.g., int8, int16, uint16, float64) from pandas objects to Spark's `DecimalType` when safe casting is permitted.

#### `test_arrow_cast_enabled_str_to_numeric`
This test checks if Arrow can successfully perform type casts from string to various numeric Spark types inside pandas UDFs without errors.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_chained_udfs_with_complex_variant`
This test checks chaining behavior between pandas UDFs returning Arrays of `VariantType` and subsequent UDFs parsing string contents from those elements, verifying that nested types are respected throughout execution.

#### `test_chained_udfs_with_variant`
This test validates that variants contained in structured objects like Arrays, Maps, and Structs maintain type consistency when passed as parameters in and out between connected UDF executions.

#### `test_datasource_with_udf`
This test ensures that pandas UDFs execute correctly on DataFrames created by Spark's standard scan file sources and Data Source V2 implementations.

#### `test_input_nested_arrays`
This test check that a pandas UDF properly reads and formats a DataFrame field that contains an array holding nested array data objects.

#### `test_input_nested_maps`
This test ensures that a pandas UDF is able to process and fetch data out of a DataFrame field containing a Map data object inside another Map data object.

#### `test_input_nested_structs`
This test verifies that a pandas UDF functions correctly when processing columns that contain deeply nested schema structures, composed by chains of Struct objects.

#### `test_kwargs`
This test checks whether Python UDF calls can map and fill named/keyword arguments correctly, validating correct parameter mapping and negative cases.

#### `test_mixed_udf`
Tests the mixture of multiple UDFs and Pandas UDFs (both scalar and iterator types) in single expressions and multiple projections, ensuring they produce the expected results.

#### `test_mixed_udf_and_sql`
Tests the mixture of UDFs and SQL expressions by calling a shared helper method.

#### `test_named_arguments`
Tests that UDFs can be called with named arguments in both DataFrame API and Spark SQL, ensuring correct parameter mapping.

#### `test_named_arguments_and_defaults`
Tests that UDFs with default arguments can be called with named arguments, handling cases with and without providing the optional argument.

#### `test_named_arguments_negative`
Tests negative cases for named arguments in UDFs, ensuring appropriate AnalysisException or PythonException are raised for duplicate or invalid arguments.

#### `test_nondeterministic_vectorized_udf`
Tests that nondeterministic UDFs are evaluated only once in chained UDF evaluations, preventing multiple executions.

#### `test_nondeterministic_vectorized_udf_in_aggregate`
Verifies that an analysis exception is raised when using a nondeterministic vectorized UDF in an aggregate function.

#### `test_pandas_array_struct`
Tests support for Array of Struct for Pandas UDFs and toPandas, verifying data integrity and type mapping.

#### `test_pandas_udf_nested_arrays`
Tests Pandas UDFs with nested array return types (ArrayType(ArrayType(StringType()))).

#### `test_pandas_udf_tokenize`
Tests a Pandas UDF that tokenizes a string into an array of strings.

#### `test_pandas_udf_with_column_vector`
Tests Pandas UDFs with off-heap column vectors enabled and disabled, ensuring correct operation with Parquet files.

#### `test_register_nondeterministic_vectorized_udf_basic`
Tests registering nondeterministic vectorized UDFs with the catalog and calling them in SQL.

#### `test_register_vectorized_udf_basic`
Tests registering basic vectorized UDFs with the catalog and using them in DataFrame queries and SQL.

#### `test_scalar_iter_pandas_udf_with_compression_codec`
Tests scalar iterator Pandas UDFs with different Arrow compression codecs (none, zstd, lz4).

#### `test_scalar_iter_pandas_udf_with_logging`
Tests that logging from within a scalar iterator Pandas UDF works and logs can be retrieved via tvf.python_worker_logs.

#### `test_scalar_iter_pandas_udf_with_single_output_batch`
Tests that a scalar iterator UDF can yield a single output batch for multiple input batches.

#### `test_scalar_iter_udf_close`
Tests that the iterator in a scalar iterator UDF is closed correctly.

#### `test_scalar_iter_udf_close_early`
Tests that GeneratorExit is caught and resources are cleaned up when a scalar iterator UDF is closed early.

#### `test_scalar_iter_udf_init`
Tests that a scalar iterator UDF can initialize state based on task context and produce deterministic results.

#### `test_scalar_pandas_udf_with_compression_codec`
Tests scalar Pandas UDFs with different Arrow compression codecs.

#### `test_scalar_pandas_udf_with_compression_codec_complex_types`
Tests scalar Pandas UDFs with compression for complex types (strings and arrays).

#### `test_scalar_pandas_udf_with_logging`
Tests that logging from within a scalar Pandas UDF works and logs can be retrieved.

#### `test_timestamp_dst`
Tests that timestamps are handled correctly across Daylight Saving Time transitions in both scalar and iterator UDFs.

#### `test_type_annotation`
Tests that type hints can be used in UDF definitions (regression test for SPARK-23569).

#### `test_udafs_with_complex_variant_input`
Tests Pandas UDFs taking complex types containing Variants (struct, array, map) as input.

#### `test_udafs_with_complex_variant_output`
Tests Pandas UDFs returning complex types containing Variants (array, map).

#### `test_udafs_with_variant_input`
Tests Pandas UDFs taking a Variant as input.

#### `test_udafs_with_variant_output`
Tests Pandas UDFs returning a Variant.

#### `test_udf_category_type`
Tests Pandas UDFs returning pandas Category type, ensuring it maps correctly to Spark's StringType.

#### `test_udf_struct_with_metadata_value_field`
Tests that structs with metadata and value fields are NOT treated as Variants if they don't match the expected schema.

#### `test_udf_with_nested_variant_input`
Tests UDFs with nested Variant input (struct, array, map).

#### `test_udf_with_variant_input`
Tests standard UDFs with Variant input.

#### `test_udf_with_variant_nested_output`
Tests UDFs returning nested Variants (struct, array, map).

#### `test_udf_with_variant_output`
Tests standard UDFs returning a Variant.

#### `test_vectorized_udf_array_type`
Tests vectorized UDFs with ArrayType.

#### `test_vectorized_udf_basic`
Tests basic functionality of vectorized UDFs with various primitive types and arrays.

#### `test_vectorized_udf_chained`
Tests chaining of vectorized UDFs.

#### `test_vectorized_udf_chained_struct_type`
Tests chaining of vectorized UDFs that return StructType.

#### `test_vectorized_udf_check_config`
Tests that spark.sql.execution.arrow.maxRecordsPerBatch is respected by vectorized UDFs.

#### `test_vectorized_udf_complex`
Tests more complex expressions involving multiple vectorized UDFs.

#### `test_vectorized_udf_datatype_string`
Tests vectorized UDFs with data types specified as strings.

#### `test_vectorized_udf_dates`
Tests vectorized UDFs with DateType, ensuring correctness and handling of nulls.

#### `test_vectorized_udf_decorator`
Tests using the @pandas_udf decorator.

#### `test_vectorized_udf_empty_partition`
Tests vectorized UDFs on an empty partition.

#### `test_vectorized_udf_exception`
Tests exception handling within vectorized UDFs.

#### `test_vectorized_udf_invalid_length`
Tests that an error is raised when a vectorized UDF returns a Series of invalid length.

#### `test_vectorized_udf_map_type`
Tests vectorized UDFs with MapType.

#### `test_vectorized_udf_nested_struct`
Tests vectorized UDFs with nested struct data types.

#### `test_vectorized_udf_null_array`
Tests vectorized UDFs with arrays containing nulls.

#### `test_vectorized_udf_null_binary`
Tests vectorized UDFs with binary data containing nulls.

#### `test_vectorized_udf_null_boolean`
Verifies that a vectorized scalar Pandas UDF handles null values correctly for boolean types.

#### `test_vectorized_udf_null_byte`
Verifies that a vectorized scalar Pandas UDF handles null values correctly for byte types.

#### `test_vectorized_udf_null_decimal`
Verifies that a vectorized scalar Pandas UDF handles null values correctly for decimal types.

#### `test_vectorized_udf_null_double`
Verifies that a vectorized scalar Pandas UDF handles null values correctly for double types.

#### `test_vectorized_udf_null_float`
Verifies that a vectorized scalar Pandas UDF handles null values correctly for float types.

#### `test_vectorized_udf_null_int`
Verifies that a vectorized scalar Pandas UDF handles null values correctly for integer types.

#### `test_vectorized_udf_null_long`
Verifies that a vectorized scalar Pandas UDF handles null values correctly for long integer types.

#### `test_vectorized_udf_null_short`
Verifies that a vectorized scalar Pandas UDF handles null values correctly for short integer types.

#### `test_vectorized_udf_null_string`
Verifies that a vectorized scalar Pandas UDF handles null values correctly for string types.

#### `test_vectorized_udf_return_scalar`
Checks that a vectorized UDF can return a scalar value by calling an internal helper method `check_vectorized_udf_return_scalar`.

#### `test_vectorized_udf_return_timestamp_tz`
Verifies that a vectorized scalar Pandas UDF handles timestamp types with time zones correctly, ensuring the returned timestamps match the expected values in the session timezone.

#### `test_vectorized_udf_string_in_udf`
Verifies that a vectorized scalar Pandas UDF returning a string correctly casts the input to a string, comparing the results with a standard Spark cast.

#### `test_vectorized_udf_struct_complex`
Verifies that a vectorized scalar Pandas UDF can return a complex struct containing timestamps and arrays, checking that the returned values are correct.

#### `test_vectorized_udf_struct_empty`
Verifies that a vectorized scalar Pandas UDF can return an empty struct, ensuring it produces the expected empty rows.

#### `test_vectorized_udf_struct_type`
Verifies that a vectorized scalar Pandas UDF can return a struct type, testing different ways to define the return type and ensuring the output matches expectations.

#### `test_vectorized_udf_struct_with_empty_partition`
Verifies that a vectorized scalar Pandas UDF returning a struct works correctly even when some partitions are empty.

#### `test_vectorized_udf_timestamps`
Verifies that a vectorized scalar Pandas UDF correctly handles timestamp types without altering them due to time zone calculations.

#### `test_vectorized_udf_timestamps_respect_session_timezone`
Verifies that a vectorized scalar Pandas UDF respects the session time zone when handling timestamp types, comparing results across different time zones.

#### `test_vectorized_udf_varargs`
Verifies that a vectorized scalar Pandas UDF can accept variable arguments (*args) and correctly processes the first argument.

#### `test_vectorized_udf_wrong_return_type`
Checks that calling a vectorized UDF with a wrong return type raises the expected exception, by calling an internal helper method.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/pandas/test_parity_pandas_udf_window.py`

### Class: `PandasUDFWindowParityTests`

#### `test_array_type`
Verifies that a Pandas grouped aggregate UDF can return an array type within a window operation.

#### `test_arrow_batch_slicing`
Tests that Arrow batch slicing works correctly with different configurations of max records and max bytes per batch when applying a Pandas UDF on a co-grouped DataFrame.

#### `test_arrow_cast_numeric_to_decimal`
Verifies that Arrow correctly casts various numeric types to a decimal type within a windowed Pandas grouped aggregate UDF.

#### `test_arrow_cast_str_to_numeric`
Verifies that Arrow correctly casts string values to numeric types (integer, long, float, double) within a windowed Pandas grouped aggregate UDF.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_bounded_mixed`
Verifies that mixed bounded window operations with different UDFs (mean, max) produce the same results as standard Spark window operations.

#### `test_bounded_simple`
Verifies that simple bounded window operations with various UDFs (mean, count, max, min) produce the same results as standard Spark window operations.

#### `test_growing_window`
Verifies that growing window operations (unbounded preceding to current row/value) with a mean UDF produce the exact same results as standard Spark window operations.

#### `test_invalid_args`
Checks that invalid arguments to a window function raise an exception by calling an internal helper method.

#### `test_kwargs`
Tests that user-defined functions can be called with keyword arguments in both Python and SQL interfaces, and that duplicate or unexpected arguments are handled correctly with exceptions.

#### `test_mixed_sql`
Verifies that mixed SQL expressions and window UDFs produce the same results as standard Spark window operations.

#### `test_mixed_sql_and_udf`
Verifies that mixing SQL window functions and window UDFs in the same expression, or chaining them, produces correct results equivalent to standard Spark operations.

#### `test_mixed_udf`
Tests the combination and chaining of multiple standard UDFs and Pandas UDFs (both scalar and iterator types) in single expressions and projections, ensuring they all produce correct results.

#### `test_multiple_udfs`
Tests that multiple registered UDFs can be used together and nested within SQL queries.

#### `test_multiple_udfs_in_single_projection`
Test multiple window aggregate pandas UDFs in a single select/projection.

#### `test_named_arguments`
Tests that named arguments can be used when calling UDFs in Python and SQL, covering different combinations and orders.

#### `test_named_arguments_negative`
Tests that invalid usage of named arguments in UDF calls (e.g., duplicate arguments, unexpected arguments) results in correct exceptions.

#### `test_replace_existing`
Verifies that a window UDF can replace an existing column with its windowed result, and it matches standard Spark behavior.

#### `test_shrinking_window`
Verifies that shrinking window operations (current row/value to unbounded following) with a mean UDF produce the same results as standard Spark window operations.

#### `test_simple`
Tests a basic retry policy with a mock stub that raises internal errors, ensuring the correct number of attempts and raised exceptions are tracked.

#### `test_sliding_window`
Verifies that sliding window operations with a mean UDF produce the same results as standard Spark window operations.

#### `test_window_pandas_udf_with_logging`
Verifies that logging from within a windowed Pandas UDF works correctly and the logs are accessible through a table-valued function.

#### `test_without_partitionBy`
Verifies that window operations without partitionBy (i.e., operating on the whole dataset) produce the same results as standard Spark window operations.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/shell/test_progress.py`

### Class: `ProgressBarTest`

#### `test_configure_char`
Verifies that the progress bar character can be configured and prints correctly.

#### `test_disabled_does_not_print`
Verifies that the progress bar does not print anything when disabled.

#### `test_finish_progress`
Verifies that finishing the progress bar leaves the last line clean.

#### `test_progress_handler`
Tests that a custom progress handler is called with correct statistics during progress updates and completion.

#### `test_simple_progress`
Tests the default behavior of the progress bar, checking percentage, character usage, and scanned bytes output.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SparkConnectProgressHandlerE2E`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_custom_handler_works`
Verifies that a custom progress handler registered with Spark Connect is actively called during query execution.

#### `test_progress_properly_recorded`
Verifies that a registered progress handler is invoked at least once during a query, indicating active progress recording.

## File: `python/pyspark/sql/tests/connect/streaming/test_parity_foreach.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `StreamingForeachParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_streaming_foreach_with_basic_open_process_close`
Verifies that a streaming query using `foreach` correctly invokes the `open`, `process`, and `close` methods of the provided writer object for each partition and epoch.

#### `test_streaming_foreach_with_invalid_writers`
Verifies that the `foreach` streaming sink correctly identifies and rejects invalid writer objects that lack required callable methods or have wrong parameter counts.

#### `test_streaming_foreach_with_open_returning_false`
Verifies that if the open method of a ForeachWriter returns False, the process method is not called, but close is still called.

#### `test_streaming_foreach_with_process_throwing_error`
Verifies that an error thrown in the process method of a ForeachWriter correctly fails the streaming query and invokes close.

#### `test_streaming_foreach_with_simple_function`
Tests that a simple function can be used with foreach in a streaming query.

#### `test_streaming_foreach_without_close_method`
Verifies that ForeachWriter works correctly even if it does not implement the close method.

#### `test_streaming_foreach_without_open_and_close_methods`
Verifies that ForeachWriter works correctly when only the process method is implemented.

#### `test_streaming_foreach_without_open_method`
Verifies that ForeachWriter works correctly even if it does not implement the open method.

## File: `python/pyspark/sql/tests/connect/streaming/test_parity_foreach_batch.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `StreamingForeachBatchParityTests`

#### `test_accessing_spark_session`
Verifies that the Spark session can be accessed within the function passed to foreachBatch.

#### `test_accessing_spark_session_through_df`
Verifies that a DataFrame defined outside foreachBatch can be accessed and used within it.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_nested_dataframes`
Tests the usage of nested DataFrames within a curried function used in foreachBatch.

#### `test_pickling_error`
Verifies that a PySparkPicklingError is raised when an unpicklable object is used inside foreachBatch.

#### `test_streaming_foreach_batch`
Basic functional test for foreachBatch, verifying it can process micro-batches and write them to a table.

#### `test_streaming_foreach_batch_external_column`
Verifies that a Column object defined outside foreachBatch can be successfully serialized and used within it.

#### `test_streaming_foreach_batch_function_calling`
Verifies that external functions can be called within the foreachBatch function.

#### `test_streaming_foreach_batch_graceful_stop`
Inherits and runs the test for graceful shutdown of a streaming query using foreachBatch.

#### `test_streaming_foreach_batch_import`
Verifies that modules imported in the main script are available within the foreachBatch function execution context.

#### `test_streaming_foreach_batch_path_access`
Verifies that reading data from paths works correctly inside the foreachBatch function.

#### `test_streaming_foreach_batch_propagates_python_errors`
Inherits and runs the test to ensure Python errors in foreachBatch are propagated correctly.

#### `test_streaming_foreach_batch_spark_session`
Verifies that the correct Spark session is accessible via df.sparkSession inside foreachBatch.

#### `test_streaming_foreach_batch_tempview`
Verifies that temporary views created within the foreachBatch function are accessible within that same batch.

#### `test_worker_initialization_error`
Verifies that an initialization error on the streaming runner is correctly reported.

## File: `python/pyspark/sql/tests/connect/streaming/test_parity_listener.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `StreamingListenerParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_listener_events_spark_command`
Verifies that StreamingQueryListener receives start, progress, and termination events.

#### `test_listener_management`
Tests adding, removing, and re-adding StreamingQueryListener instances.

#### `test_listener_observed_metrics`
Verifies that metrics observed using the observe API are accessible within the listener's progress events.

#### `test_listener_throw`
Following classic Spark's behavior, when the callback of user-defined listener throws,
other listeners should still proceed.

#### `test_server_listener_uninterruptible`
Verifies that interrupting queries on the server does not prevent the listener from receiving termination events.

#### `test_slow_query`
Verifies that listeners work correctly and receive events for slow streaming queries.

#### `test_streaming_progress`
Should be able to access fields using attributes in lastProgress / recentProgress
e.g. q.lastProgress.id

## File: `python/pyspark/sql/tests/connect/streaming/test_parity_streaming.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `StreamingParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_invalid_name_wrong_type`
Test that None and non-string types are rejected.

#### `test_invalid_names`
Test that various invalid source names are rejected.

#### `test_name_before_format`
Test that order doesn't matter - name can be set before format.

#### `test_name_method_chaining`
Test that name() returns the reader for method chaining.

#### `test_name_with_different_formats`
Test that name() works with different streaming data sources.

#### `test_name_with_valid_names`
Test that various valid source name patterns work correctly.

#### `test_query_manager_await_termination`
Tests the awaitAnyTermination method of StreamingQueryManager.

#### `test_query_manager_get`
Verifies that StreamingQueryManager.get() can retrieve an active streaming query by its ID.

#### `test_query_manager_no_recreation`
Verifies that calling spark.streams returns the same instance instead of creating a new one.

#### `test_stream_await_termination`
Tests the awaitTermination method on an individual StreamingQuery.

#### `test_stream_exception`
Verifies that exception() returns None for successful queries and the correct exception for failed ones.

#### `test_stream_read_options`
Verifies that options specified during readStream are correctly applied.

#### `test_stream_read_options_overwrite`
Verifies options overwrite behavior for streaming read when legacy path option behavior is enabled.

#### `test_stream_real_time_trigger`
Verifies that an expected exception is raised when using an unsupported real-time trigger.

#### `test_stream_save_options`
Verifies that options specified in writeStream are correctly used to save the stream.

#### `test_stream_save_options_overwrite`
Verifies overwrite behavior for save options when legacy path option behavior is enabled.

#### `test_stream_status_and_progress`
Verifies that a StreamingQuery provides status information and progress updates.

#### `test_stream_trigger`
Tests the trigger method of DataStreamWriter for correct argument handling.

#### `test_streaming_drop_duplicate_within_watermark`
This verifies dropDuplicatesWithinWatermark works with a streaming dataframe.

#### `test_streaming_progress`
Should be able to access fields using attributes in lastProgress / recentProgress
e.g. q.lastProgress.id

#### `test_streaming_query_functions_basic`
Sanity checks basic methods and properties of a StreamingQuery.

#### `test_streaming_query_name_edge_case`
Verifies edge cases for query names (default None, error on empty string).

#### `test_streaming_read_from_table`
Verifies that reading from a table as a stream works.

#### `test_streaming_with_temporary_view`
This verifies createOrReplaceTempView() works with a streaming dataframe. An SQL
SELECT query on such a table results in a streaming dataframe and the streaming query works
as expected.

#### `test_streaming_write_to_table`
Verifies that streaming output can be written to a table using toTable.

#### `test_streaming_write_to_table_cluster_by`
Verifies that writing to a table using toTable works correctly with clusterBy.

## File: `python/pyspark/sql/tests/connect/test_connect_basic.py`

### Class: `ReusedMixedTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SparkConnectBasicTests`

#### `test_alias`
Testing supported and unsupported alias

#### `test_alias_metadata`
Verifies that metadata can be attached to a DataFrame column and subsequently cleared.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_column_regexp`
Verifies the behavior of colRegex against a standard regular expression.

#### `test_count`
Verifies that the count method on a DataFrame returns the correct number of rows.

#### `test_create_global_temp_view`
Tests creating and replacing global temporary views and error handling for existing views.

#### `test_create_session_local_temp_view`
Tests creating and replacing session-local temporary views.

#### `test_crossjoin`
Verifies that both crossJoin() and join(..., how='cross') return correct results.

#### `test_deduplicate`
Verifies that distinct and dropDuplicates correctly remove duplicate rows.

#### `test_df_caache`
Verifies that DataFrame caching marks the DataFrame as cached.

#### `test_df_get_item`
Tests the __getitem__ method of a DataFrame for filtering, selecting columns by name and index, and error handling for invalid input types.

#### `test_df_getattr_behavior`
Tests how the DataFrame handles accessing attributes, including columns that do not exist, and how this behavior is affected by the PYSPARK_VALIDATE_COLUMN_NAME_LEGACY environment variable.

#### `test_drop`
Tests the drop method of a DataFrame for removing specific columns.

#### `test_empty_dataset`
Tests that converting an empty DataFrame to a Pandas DataFrame works correctly and produces an empty Pandas DataFrame with correct columns.

#### `test_explain_string`
Tests the explain functionality of a DataFrame to get execution plans as a string.

#### `test_extended_hint_types`
Tests that the hint method accepts various parameter types and correctly rejects invalid ones like dictionaries.

#### `test_hint`
Verifies the behavior of the hint method with different types of hints and parameters, including error conditions.

#### `test_input_files`
Verifies that the inputFiles() method returns the correct list of source files used by the DataFrame.

#### `test_is_empty`
Checks the isEmpty() method to correctly identify if a DataFrame has no rows.

#### `test_is_empty_with_unsupported_types`
Validates that isEmpty() works even when the DataFrame contains complex or unsupported data types.

#### `test_is_local`
Tests the isLocal() method to determine if the DataFrame can be evaluated locally.

#### `test_is_streaming`
Verifies that the isStreaming property correctly identifies if a DataFrame is a streaming data source.

#### `test_join_ambiguous_cols`
Tests joining DataFrames with columns that have identical names to ensure correct handling of potential ambiguities.

#### `test_join_condition_column_list_columns`
Tests join operations with explicit column conditions and lists of conditions.

#### `test_join_hint`
Tests that join hints (like BROADCAST or MERGE) are correctly parsed and affect the execution plan as expected.

#### `test_join_with_cte`
Verifies that joining a DataFrame with a subquery defined via a CTE works correctly.

#### `test_limit_offset`
Tests the combination of limit and offset methods to paginate or slice DataFrame results.

#### `test_namedargs_with_global_limit`
Verifies that passing named arguments to SQL queries and using them with limits works as intended.

#### `test_observe`
Tests the observe method for computing aggregate metrics on the DataFrame.

#### `test_parse_col_name`
Tests the internal parser for column names, particularly handling complex names with backticks.

#### `test_plan_compression`
Verifies that large query plans are automatically compressed for efficient transmission over the network.

#### `test_print_schema`
Validates that the schema's tree string matches the expected structure.

#### `test_range`
Tests the range function for creating a DataFrame with a sequence of numbers.

#### `test_repartition`
Tests repartition and coalesce methods to change the number of partitions.

#### `test_repartition_by_expression`
Tests repartitioning a DataFrame based on specific column expressions.

#### `test_repartition_by_range`
Tests repartitioning a DataFrame by a range of values based on specific columns.

#### `test_repr`
Tests that the string representation of a Connect DataFrame matches the classic Spark DataFrame.

#### `test_same_semantics`
Tests the sameSemantics method to determine if two DataFrames represent the same logical query plan.

#### `test_schema`
Comprehensive tests ensuring that schemas created in Spark Connect match those from classic Spark for various complex data types.

#### `test_select_expr`
Tests the selectExpr method for selecting columns using SQL-like expressions.

#### `test_select_star`
Tests selecting all columns using star expansion, including within nested structures.

#### `test_self_join`
Tests performing a self-join on a DataFrame.

#### `test_semantic_hash`
Verifies that equivalent DataFrame queries produce the same semantic hash.

#### `test_serialization`
Validates that DataFrames can be serialized and deserialized using cloudpickle.

#### `test_serialization_II`
Validates that DataFrames can be serialized and deserialized using CPickleSerializer.

#### `test_session`
Tests that retrieving the Spark session from a DataFrame returns the correct session instance.

#### `test_show`
Validates that the output of DataFrame show() produces the expected tabular string format.

#### `test_simple_explain_string`
Basic verification that _explain_string returns non-empty output.

#### `test_simple_transform`
SPARK-41203: Support DF.transform

#### `test_sort`
Tests the sort method with various column specifications and ordering directions.

#### `test_sql`
Basic test that execution of a SQL query produces expected rows.

#### `test_sql_with_command`
Tests that commands like show functions via SQL return expected results.

#### `test_sql_with_invalid_args`
Verifies that passing invalid types as arguments to spark.sql raises the appropriate type error.

#### `test_sql_with_named_args`
Verifies that named arguments in SQL queries are correctly resolved and substituted.

#### `test_sql_with_pos_args`
Verifies that positional arguments in SQL queries are correctly resolved.

#### `test_subquery_alias`
Tests that the alias method correctly creates an alias for the DataFrame in the query plan.

#### `test_tail`
Tests the tail method to retrieve the last rows of a DataFrame.

#### `test_to`
Tests the to method for changing the schema of the DataFrame to a specific structure, including success and failure cases.

#### `test_toDF`
Tests the toDF method for renaming columns of the DataFrame.

#### `test_toJSON`
Validates that toJSON() correctly converts DataFrame rows into JSON formatted strings.

#### `test_truncate_message`
Verifies that internal protocol messages describing operations are correctly truncated based on provided size limits.

#### `test_union_by_name`
Verifies that DataFrame unionByName works correctly in Spark Connect, matching classic Spark behavior for both normal unions and unions allowing missing columns.

#### `test_verify_col_name`
Validates the internal verify_col_name helper function against a complex schema containing nested structs and arrays, testing various quoting and dot-separated paths.

#### `test_version`
Verifies that the Spark Connect session reports the same Spark version as the classic Spark session.

#### `test_window_spec_serialization`
Verifies that a Spark Connect WindowSpec object can be successfully serialized and deserialized using Python's pickle.

#### `test_with_columns`
Verifies that withColumn and withColumns APIs work correctly in Spark Connect, matching classic Spark.

#### `test_with_columns_renamed`
Verifies that withColumnRenamed and withColumnsRenamed APIs work correctly in Spark Connect, matching classic Spark.

#### `test_with_metadata`
Verifies that the withMetadata API allows setting column metadata and correctly raises an error when an invalid metadata type is provided.

### Class: `SparkConnectGCTests`

#### `test_arrow_batch_result_chunking`
Verifies that the Spark Connect server correctly chunks Arrow batch results based on either client preferred size or server-side max chunk size configuration.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_garbage_collection_checkpoint`
Verifies that garbage-collecting a DataFrame in the client triggers the removal of its corresponding cached state in the Spark Connect server.

#### `test_garbage_collection_derived_checkpoint`
Verifies that the cached state in the Spark Connect server is preserved as long as DataFrames derived from the original checkpointed DataFrame still exist.

### Class: `SparkConnectSQLTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_connect_channel.py`

### Class: `ChannelBuilderTests`

#### `test_channel_options`
Verifies that the DefaultChannelBuilder correctly processes and overrides default gRPC channel options.

#### `test_channel_properties`
Verifies that DefaultChannelBuilder correctly parses connection string parameters to set properties like endpoint, security, and user agent.

#### `test_invalid_connection_strings`
Verifies that DefaultChannelBuilder correctly rejects invalid connection strings with a PySparkValueError.

#### `test_metadata`
Verifies that DefaultChannelBuilder correctly extracts custom headers and parameters from the connection string to produce gRPC metadata.

#### `test_metadata_with_session_id`
Verifies that DefaultChannelBuilder handles session_id properly, including UUID format validation, and ensures fixed parameters are not included in custom metadata.

#### `test_sensible_defaults`
Verifies that DefaultChannelBuilder sets appropriate default values for security (based on token presence) and user agent format.

#### `test_user_agent`
Verifies that DefaultChannelBuilder correctly parses and includes custom user agents specified in the connection string.

#### `test_user_agent_len`
Verifies that DefaultChannelBuilder enforces the maximum allowed length for user agent strings.

#### `test_valid_channel_creation`
Verifies that DefaultChannelBuilder can successfully construct valid grpc.Channel instances for various valid connection strings.

## File: `python/pyspark/sql/tests/connect/test_connect_clone_session.py`

### Class: `SparkConnectCloneSessionTest`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_clone_session_auto_generated_id`
Test that cloneSession() without arguments generates a valid UUID.

#### `test_clone_session_basic`
Test basic session cloning functionality.

#### `test_clone_session_preserves_temp_views`
Test that temporary views are preserved in cloned sessions.

#### `test_clone_session_with_custom_id`
Test cloning session with a custom session ID.

#### `test_invalid_session_id_format`
Test that invalid session ID format raises an exception.

#### `test_temp_views_independence_after_cloning`
Test that temp views are cloned and then can be modified independently.

### Class: `SparkConnectSQLTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_connect_collection.py`

### Class: `ReusedMixedTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SparkConnectCollectionTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_collect`
Verifies that the collect API returns correct data matching classic Spark, and that the returned Row objects contain the expected column names in their schema.

#### `test_collect_binary_type`
Test that df.collect() respects binary_as_bytes configuration for server-side data

#### `test_collect_nested_type`
Verifies that collect works correctly for DataFrames containing complex nested structures like arrays, maps, and nested structs.

#### `test_collect_timestamp`
Verifies that timestamp data can be successfully collected and that the date_trunc function works as expected.

#### `test_first`
Verifies that the first API correctly returns the first row of a DataFrame or None for an empty DataFrame.

#### `test_foreach_partition_binary_type`
Test that df.foreachPartition() respects binary_as_bytes configuration

Since foreachPartition() runs on executors and cannot return data to the driver,
we test by ensuring the function doesn't throw exceptions when it expects the correct types.

#### `test_head`
Verifies that the head API correctly returns the requested number of leading rows.

#### `test_take`
Verifies that the take API correctly returns the requested number of rows as a list.

#### `test_to_local_iterator_binary_type`
Test that df.toLocalIterator() respects binary_as_bytes configuration

#### `test_to_pandas`
Verifies that the toPandas API correctly converts Spark Connect DataFrames to Pandas DataFrames, matching classic Spark results across various data types and null handling.

## File: `python/pyspark/sql/tests/connect/test_connect_column.py`

### Class: `ReusedMixedTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SparkConnectColumnTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_between`
Verifies that the between operator on columns works correctly for various types including numbers, timestamps, dates, and decimals.

#### `test_cast`
Verifies that Column cast and astype work correctly for various target data types and that invalid cast types trigger a PySparkTypeError.

#### `test_cast_default_column_name`
Verifies that casting literal values results in identical auto-generated column names in both Spark Connect and classic Spark.

#### `test_column_accessor`
Verifies that struct fields, map items, and array elements can be accessed from a column using various standard syntax options.

#### `test_column_arithmetic_ops`
Verifies that standard arithmetic operators (+, -, *, /, %, **) work correctly on columns, including with literals.

#### `test_column_bitwise_ops`
Verifies that bitwise AND, OR, and XOR operations on columns produce results matching classic Spark.

#### `test_column_field_ops`
Verifies that withField and dropFields methods for manipulating struct fields work correctly and enforce type rules.

#### `test_column_operator`
Verifies that the not-equal operator (!=) is supported on Column objects in Spark Connect.

#### `test_column_string_ops`
Verifies that string-matching operators startswith, endswith, and contains on columns work as expected.

#### `test_column_with_null`
Verifies that null-checking operators isNull, isNotNull, and null-safe equality eqNullSafe work correctly on columns.

#### `test_columns`
Verifies the columns property and various column manipulation/filter operations like string matching, substring extraction, and null checking, while validating argument type enforcement.

#### `test_datetime`
Verifies that columns containing dates or timestamps can be compared against standard Python datetime.date and datetime.datetime objects.

#### `test_decimal`
Verifies that numeric columns can be compared against Python decimal.Decimal objects correctly.

#### `test_distributed_sequence_id`
Verifies the behavior of the DistributedSequenceID expression in creating a sequence of IDs.

#### `test_invalid_ops`
Verifies that applying invalid operators (like in, and, or, not as Python operators, or iteration) directly on Column objects raises appropriate errors.

#### `test_isin`
Verifies that the isin operator works correctly with various collection types (tuples, lists, sets) and Column references.

#### `test_lambda_str_representation`
Verifies that the string representation of a column constructed using a lambda function (like in array_sort) is correctly generated and doesn't vary unexpectedly.

#### `test_literal_integers`
Verifies that integer literals of various sizes are created correctly within valid ranges, and that values exceeding the 64-bit long limit raise errors.

#### `test_literal_null`
Verifies that LiteralExpression can be created with a None value for a wide range of supported data types.

#### `test_literal_with_acceptable_type`
Verifies that LiteralExpression accepts and correctly handles standard Python types for corresponding Spark data types.

#### `test_literal_with_unsupported_type`
Verifies that LiteralExpression correctly asserts and fails when initialized with Python values that don't match the specified Spark data type or fall outside valid ranges.

#### `test_none`
Verifies that comparing columns to Python None using standard comparison operators produces results matching classic Spark.

#### `test_simple_binary_expressions`
Test complex expression

#### `test_transform`
Verifies that the transform method on columns works correctly with both standard built-in functions and custom lambda functions.

#### `test_with_field_column_name`
Verifies that using withField to update a struct field and then selecting a child field produces the correct result matching classic Spark.

## File: `python/pyspark/sql/tests/connect/test_connect_creation.py`

### Class: `ReusedMixedTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SparkConnectCreationTests`

#### `test_array_has_nullable`
Tests creating DataFrames with array types containing non-nullable and nullable elements, comparing Spark Connect and classic Spark behavior for schema and data correctness.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_cast_with_ddl`
Tests creating a DataFrame with a DDL-formatted schema string, verifying that the schema is correctly applied and matches between Spark Connect and classic Spark.

#### `test_create_dataframe_from_arrays`
Tests creating DataFrames from Python array.array objects, verifying schema and data parity between Spark Connect and classic Spark.

#### `test_create_dataframe_from_pandas_with_ns_timestamp`
Truncate the timestamps for nanoseconds.

#### `test_create_dataframe_with_coercion`
Tests creating DataFrames where data types need coercion (e.g., float/string to common type), verifying parity between Spark Connect and classic Spark.

#### `test_create_df_from_objects`
Tests creating a DataFrame from a list of custom objects, verifying schema and data parity between Spark Connect and classic Spark.

#### `test_create_df_nullability`
Tests that creating a DataFrame with data violating schema nullability constraints raises a PySparkValueError in classic Spark.

#### `test_create_empty_df`
Tests creating empty DataFrames with various schema specifications, and verifies that attempting to infer schema from empty data raises a CANNOT_INFER_EMPTY_SCHEMA error.

#### `test_from_empty_pandas_dataframe`
Tests that creating a DataFrame from an empty Pandas DataFrame raises a CANNOT_INFER_EMPTY_SCHEMA error.

#### `test_from_pandas_dataframe_with_zero_columns`
SPARK-55350: Test that row count is preserved when creating DataFrame from
pandas with 0 columns but with explicit schema in Spark Connect.

#### `test_large_client_data`
Tests that Spark Connect can handle creating DataFrames with large amounts of client-side data (exceeding typical 4MB gRPC limits).

#### `test_map_has_nullable`
Tests creating DataFrames with map types containing non-nullable and nullable values, comparing Spark Connect and classic Spark behavior.

#### `test_nested_type_create_from_rows`
Tests creating DataFrames with deeply nested structures (structs, arrays, maps) from lists of Rows and dictionaries, verifying schema and data parity.

#### `test_schema_has_nullable`
Tests that the nullability property of schema fields is preserved when creating DataFrames, including when round-tripping through Pandas.

#### `test_simple_udt`
Tests creating DataFrames with User Defined Types (UDTs) like ML Vector and Matrix, verifying schema parity for empty data.

#### `test_streaming_local_relation`
Tests that local relations larger than the threshold are correctly handled, likely involving caching or streaming to the server.

#### `test_struct_has_nullable`
Tests creating DataFrames with struct types containing non-nullable and nullable fields, comparing Spark Connect and classic Spark.

#### `test_timestampe_create_from_rows`
Tests creating a DataFrame with timestamp data from local rows, verifying parity.

#### `test_with_atom_type`
Tests creating DataFrames with atomic types (long, int, short) from simple Python lists, verifying parity.

#### `test_with_local_data`
SPARK-41114: Test creating a dataframe using local data

#### `test_with_local_list`
SPARK-41446: Test creating a dataframe using local list

#### `test_with_local_ndarray`
SPARK-41446: Test creating a dataframe using local list

#### `test_with_local_rows`
Tests creating DataFrames from lists of Row objects and dictionaries, with and without renaming columns, verifying parity.

#### `test_with_none_and_nan`
Tests creating DataFrames with None and NaN values, and tests related functions like eqNullSafe, nanvl, and pmod for parity.

## File: `python/pyspark/sql/tests/connect/test_connect_dataframe_property.py`

### Class: `ReusedMixedTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SparkConnectDataFramePropertyTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_cached_property_is_copied`
Tests that cached properties like schema are correctly copied and do not affect the original DataFrame when modified.

#### `test_cached_schema_cogroup_apply_in_arrow`
Tests that the cached schema is correctly set and matches after a cogroup.applyInArrow operation.

#### `test_cached_schema_cogroup_apply_in_pandas`
Tests that the cached schema is correctly set and matches after a cogroup.applyInPandas operation.

#### `test_cached_schema_group_apply_in_arrow`
Tests that the cached schema is correctly set and matches after a groupby().applyInArrow operation.

#### `test_cached_schema_group_apply_in_pandas`
Tests that the cached schema is correctly set and matches after a groupby().applyInPandas operation, also testing schema specified as a string.

#### `test_cached_schema_in_chain_op`
Tests that the cached schema is preserved across a chain of operations like withColumn, where, repartition, distinct, and sample.

#### `test_cached_schema_map_in_arrow`
Tests that the cached schema is correctly set and matches after a mapInArrow operation.

#### `test_cached_schema_map_in_pandas`
Tests that the cached schema is correctly set and matches after a mapInPandas operation.

#### `test_cached_schema_set_op`
Tests how schema caching behaves across set operations like union, intersect, and subtract, ensuring it is only inferred when possible.

#### `test_cached_schema_to`
Tests that the cached schema is correctly updated after applying a new schema via the to() method.

## File: `python/pyspark/sql/tests/connect/test_connect_error.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SparkConnectErrorTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_column_cannot_be_constructed_from_string`
Tests that directly constructing a Spark Connect Column from a string raises a TypeError.

#### `test_deduplicate_within_watermark_in_batch`
Tests that dropDuplicatesWithinWatermark raises an AnalysisException when called on a batch (non-streaming) DataFrame.

#### `test_different_spark_session_join_or_union`
Tests that attempting to join or union DataFrames from different Spark sessions raises a SessionNotSameException.

#### `test_error_handling`
Tests basic error handling in Spark Connect, specifically that selecting a non-existent column raises an AnalysisException.

#### `test_invalid_column`
Tests various scenarios of resolving invalid columns (e.g., using a column from another DataFrame) and verifies that appropriate AnalysisException errors are raised.

#### `test_invalid_star`
Tests invalid uses of the star (*) operator in selections and joins, verifying that appropriate AnalysisException errors are raised.

#### `test_recursion_handling_for_plan_logging`
SPARK-45852 - Test that we can handle recursion in plan logging.

#### `test_select_none`
Tests that passing None to select() raises a PySparkTypeError with expected error details.

#### `test_unsupported_functions`
Tests that attempting to access unsupported properties like rdd on a Spark Connect DataFrame raises a NotImplementedError.

#### `test_unsupported_jvm_attribute`
Tests that accessing unsupported internal JVM attributes on Session, DataFrame, Reader, and Column raises PySparkAttributeError with JVM_ATTRIBUTE_NOT_SUPPORTED class.

#### `test_ym_interval_in_collect`
Tests that collecting a YearMonthIntervalType raises a PySparkTypeError due to lack of support in Python arrow conversion.

## File: `python/pyspark/sql/tests/connect/test_connect_function.py`

### Class: `ReusedMixedTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SparkConnectFunctionTests`

#### `test_aggregation_functions`
Tests a wide variety of aggregation functions (avg, sum, count, min, max, etc.) comparing Spark Connect and classic Spark results.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_broadcast`
Tests the broadcast hint function in joins, and verifies that passing non-DataFrames raises a TypeError.

#### `test_call_udf`
Tests calling built-in functions via call_udf, verifying parity.

#### `test_collection_functions`
Tests a large number of collection functions (arrays, maps) like array_contains, slice, sort_array, etc., verifying parity.

#### `test_count_star`
Tests count(*) and variants in selections and group-by aggregations, verifying parity.

#### `test_csv_functions`
Tests CSV-related functions like from_csv, to_csv, and schema_of_csv, verifying parity.

#### `test_date_ts_functions`
Tests a large number of date and timestamp functions (year, month, current_date, date_add, etc.), verifying parity.

#### `test_function_parity`
Tests that the set of functions available in pyspark.sql.functions matches those in pyspark.sql.connect.functions, with allowed exclusions.

#### `test_generator_functions`
Tests generator functions like explode, posexplode, inline, and flatten with arrays and maps, verifying parity.

#### `test_json_functions`
Tests JSON-related functions like from_json, to_json, get_json_object, etc., verifying parity.

#### `test_lambda_functions`
Tests higher-order functions that take lambdas (exists, aggregate, filter, transform, etc.) on arrays and maps, verifying parity.

#### `test_map_collection_functions`
Tests Spark Connect map collection functions by comparing their results with standard Spark on a sample dataset.

#### `test_math_functions`
Verifies that a wide range of math functions in Spark Connect produce the same results as in standard Spark.

#### `test_misc_functions`
Tests miscellaneous functions (including hash, cryptographic, and error-raising functions) in Spark Connect against standard Spark.

#### `test_nested_lambda_function`
Validates that Spark Connect correctly handles nested lambda functions within a transform operation.

#### `test_non_deterministic_with_seed`
Checks that non-deterministic functions like rand, randn, and shuffle behave consistently when evaluated multiple times on the same row.

#### `test_normal_functions`
Tests a variety of standard functions (bitwise, coalesce, expression evaluation, null/nan checks, etc.) in Spark Connect.

#### `test_pandas_udf_import`
Verifies that the pandas_udf function is accessible in the Spark Connect functions module.

#### `test_sort_with_nulls_order`
Tests sorting functions with various null ordering options in Spark Connect against standard Spark.

#### `test_sorting_functions_with_column`
Validates that sorting functions accept both string and Column inputs and return correct Column types.

#### `test_string_functions_multi_args`
Tests multi-argument string functions in Spark Connect against standard Spark.

#### `test_string_functions_one_arg`
Tests single-argument string functions in Spark Connect against standard Spark.

#### `test_time_window_functions`
Tests time window and session window functions in Spark Connect, including error handling for incorrect types.

#### `test_udf`
Tests the creation and usage of Python UDFs in Spark Connect, including usage as decorators and with different return type specifications.

#### `test_udtf`
Tests the creation, direct usage, and SQL registration of Python UDTFs in Spark Connect.

#### `test_when_otherwise`
Tests the when and otherwise conditional column functions in Spark Connect, including chained calls and error cases.

#### `test_window_functions`
Comprehensively tests window functions and window frame specifications (rows vs range) in Spark Connect.

#### `test_window_order`
Tests window functions with specific partitioning and ordering rules in Spark Connect.

#### `test_xml_functions`
Tests XML processing functions in Spark Connect, including parsing, schema inference, and serialization.

## File: `python/pyspark/sql/tests/connect/test_connect_plan.py`

### Class: `SparkConnectPlanTests`

#### `test_all_the_plans`
A basic plan generation test that reads a table, applies select, filter, and sort, and verifies that the protobuf plan is valid.

#### `test_binary_literal`
Verifies that binary literals are correctly represented in the generated Spark Connect plan.

#### `test_coalesce_and_repartition`
Tests the plan representation for coalesce and repartition operations, including validation of partition numbers.

#### `test_column_alias`
Tests that column aliases and their associated metadata are correctly captured in the plan.

#### `test_column_expressions`
Test a more complex combination of expressions and their translation into
the protobuf structure.

#### `test_column_literals`
Verifies that integer and long literals are correctly represented in the plan.

#### `test_column_regexp`
Tests that regular expression column references are correctly identified in the plan.

#### `test_crossjoin`
Verifies that crossJoin and standard joins with cross type produce identical execution plans.

#### `test_crosstab`
Tests the plan generation for the crosstab operation accessed via both DataFrame and stat accessor.

#### `test_datasource_read`
Tests that reading from a data source with custom options and schema correctly populates the plan.

#### `test_datetime_literal_types`
Test the different timestamp, date, time, and timedelta types.

#### `test_deduplicate`
Tests the plan generation for duplicate removal operations, both all-column and subset-based.

#### `test_describe`
Tests plan generation for the describe operation with and without specified columns.

#### `test_drop`
Tests plan generation for dropping columns, using both string names and Column objects.

#### `test_drop_na`
Tests plan generation for removing rows with missing values, covering different thresholds and subsets.

#### `test_except`
Tests plan generation for the exceptAll set operation.

#### `test_fill_na`
Tests plan generation for filling missing values with constants or dictionaries.

#### `test_filter`
Tests plan generation for filtering operations.

#### `test_filter_with_string_expr`
SPARK-41297: filter supports SQL expression

#### `test_float_nan_inf`
Tests that NaN and infinity values are correctly handled as literals in the plan.

#### `test_freqItems`
Tests plan generation for finding frequent items.

#### `test_intersect`
Tests plan generation for intersect and intersectAll operations.

#### `test_join_condition`
Tests plan generation for joins with complex conditions.

#### `test_join_using_columns`
Tests plan generation for equi-joins using specific column names.

#### `test_join_with_join_type`
Validates that all supported join types are correctly translated into the plan.

#### `test_limit`
Tests plan generation for the limit operation.

#### `test_list_to_literal`
Test conversion of lists to literals

#### `test_literal_expression_with_arrays`
Tests that array literals (including nested arrays) are correctly represented in the plan.

#### `test_literal_to_any_conversion`
Tests the two-way conversion between Python values and Spark Connect plan literals.

#### `test_melt`
Tests plan generation for the melt (unpivot) operation.

#### `test_null_literal`
Verifies that null values are correctly represented as literals in the plan.

#### `test_numeric_literal_types`
Tests that various numeric types are correctly handled as literals in the plan.

#### `test_observe`
Tests plan generation for observing metrics on a DataFrame.

#### `test_offset`
Tests plan generation for the offset operation.

#### `test_print`
Tests the string representation/printing of Spark Connect plans.

#### `test_random_split`
Tests plan generation for splitting a DataFrame randomly based on weights.

#### `test_range`
Tests the `range` method in Spark Connect, verifying that the generated protobuf plan correctly reflects the start, end, step, and number of partitions parameters.

#### `test_relation_alias`
Tests the `alias` method for DataFrames in Spark Connect, verifying that the generated protobuf plan correctly sets the subquery alias.

#### `test_relation_changes`
Tests the `changes` method of `DataFrameReader` with options in Spark Connect, verifying that the generated protobuf plan correctly captures the table identifier and provided options.

#### `test_relation_changes_no_options`
Tests the `changes` method of `DataFrameReader` without options in Spark Connect, verifying that the generated protobuf plan correctly captures the table identifier and has no options.

#### `test_relation_changes_oneof_is_relation_changes`
Tests that the `changes` method of `DataFrameReader` generates a protobuf plan where the relation type is correctly identified as 'relation_changes'.

#### `test_relation_changes_plan_print`
Tests the `print` method of the `RelationChanges` plan class in Spark Connect, verifying that it includes expected strings like 'RelationChanges' and the table name.

#### `test_relation_changes_streaming`
Tests the creation of a streaming `RelationChanges` plan in Spark Connect, verifying that the generated protobuf plan correctly marks it as a streaming relation.

#### `test_relation_changes_streaming_via_stream_reader`
Tests the `changes` method on `DataStreamReader` in Spark Connect, verifying that the generated protobuf plan correctly identifies the relation as streaming and captures the provided options.

#### `test_relation_changes_with_timestamp_options`
Tests the `changes` method of `DataFrameReader` with timestamp options in Spark Connect, verifying that the generated protobuf plan correctly captures the starting and ending timestamp options.

#### `test_repartition_by_expression`
Tests the `repartition` method with expressions in Spark Connect, verifying that the generated protobuf plan correctly reflects the number of partitions and the partitioning expressions.

#### `test_repartition_by_range`
Tests the `repartitionByRange` method in Spark Connect, verifying that the generated protobuf plan correctly reflects the number of partitions and the range partitioning expressions.

#### `test_replace`
Tests the `replace` and `na.replace` APIs in Spark Connect, verifying that the generated protobuf plan correctly captures the target columns and replacement values for various input types like scalars, tuples, and lists.

#### `test_repr`
Tests the `__repr_html__` method for plans in Spark Connect, verifying that it generates expected HTML representations for SQL and Range plans.

#### `test_sample`
Tests the `sample` method in Spark Connect, verifying that the generated protobuf plan correctly reflects the fraction, withReplacement, seed, and deterministic_order parameters for different method calls.

#### `test_select_with_columns_and_strings`
Tests the `select` method with combinations of string column names, column objects, and wildcards in Spark Connect, ensuring they produce valid plans.

#### `test_simple_column_expressions`
Tests different ways to create column expressions (attribute access, item access, and the `col` function) in Spark Connect, verifying that they produce equivalent protobuf representations, taking note of plan_id differences.

#### `test_simple_project`
Tests a simple project operation by reading a table in Spark Connect, verifying that the root relation is a read operation.

#### `test_sort`
Tests the `sort`, `orderBy`, and `sortWithinPartitions` methods in Spark Connect, verifying that the generated protobuf plans correctly reflect the sort order, direction, null ordering, and whether the sort is global or local.

#### `test_sql_project`
Tests that executing a SQL query in Spark Connect generates a protobuf plan containing the original SQL query string.

#### `test_subtract`
Tests the `subtract` method in Spark Connect, verifying that the generated protobuf plan correctly translates it to a non-all except operation.

#### `test_summary`
Tests the `summary` method in Spark Connect, verifying that the generated protobuf plan correctly reflects requested statistics or an empty list for default statistics.

#### `test_to`
Tests the `to` method with a specified schema in Spark Connect, verifying that the generated protobuf plan correctly captures the target schema.

#### `test_union`
Tests that `union` and `unionByName` correctly generate Spark Connect plan with `SET_OP_TYPE_UNION`.

#### `test_unpivot`
Tests that `unpivot` correctly generates Spark Connect plan with specified IDs, values, and column names.

#### `test_uuid_literal`
Verifies that passing a UUID as a literal raises a `TypeError`.

#### `test_write_operation`
Tests that `WriteOperation` correctly translates mode, source, path, table, and bucketing to the proto plan, and raises `ValueError` for unknown modes.

### Class: `TestObservationMerging`

#### `test_collect_metrics_with_duplicate_observation_name`
Tests that `CollectMetrics` correctly returns observations when there are duplicate names.

#### `test_join_with_distinct_observations`
Tests that `Join` correctly merges observations from left and right children.

#### `test_join_with_duplicate_observation_names`
Tests that `Join` correctly handles duplicate observation names by merging them.

#### `test_set_operation_with_duplicate_observation_names`
Tests that `SetOperation` correctly handles duplicate observation names by merging them.

## File: `python/pyspark/sql/tests/connect/test_connect_readwriter.py`

### Class: `SparkConnectReadWriterTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_csv`
Tests reading and writing CSV files, comparing results between Connect and standard Spark.

#### `test_json`
Tests reading and writing JSON files with various options and schemas, comparing Connect and standard Spark.

#### `test_json_with_dataframe_input`
Tests reading JSON from a DataFrame containing JSON strings.

#### `test_json_with_dataframe_input_and_schema`
Tests reading JSON from a DataFrame with a specified schema.

#### `test_json_with_dataframe_input_multiple_columns`
Tests reading JSON from a DataFrame that has multiple columns, verifying only the JSON column is used.

#### `test_json_with_dataframe_input_non_string_column`
Verifies that reading JSON from a DataFrame with a non-string column raises an exception.

#### `test_json_with_dataframe_input_zero_columns`
Verifies that reading JSON from a DataFrame with zero columns raises an exception.

#### `test_multi_paths`
Tests reading text and JSON from multiple paths simultaneously.

#### `test_orc`
Tests reading and writing ORC files, comparing Connect and standard Spark.

#### `test_parquet`
Tests reading and writing Parquet files, comparing Connect and standard Spark.

#### `test_parquet_compression_option`
Verifies that compression options are correctly applied when writing Parquet files.

#### `test_simple_datasource_read`
Tests reading text data source with different schema specifications.

#### `test_simple_read`
Tests basic table reading and limit application.

#### `test_simple_read_without_schema`
SPARK-41300: Schema not set when reading CSV.

#### `test_simple_udt_from_read`
Tests reading Parquet files containing User Defined Types (UDTs) in various structures (direct, array, map).

#### `test_stream_reader_invalid_name_wrong_type`
Test that None and non-string types are rejected.

#### `test_stream_reader_invalid_names`
Test that various invalid source names are rejected.

#### `test_stream_reader_name_before_format`
Test that order doesn't matter - name can be set before format.

#### `test_stream_reader_name_method_chaining`
Test that name() returns the reader for method chaining.

#### `test_stream_reader_name_persists_through_query`
Test that the name persists when starting a streaming query.

#### `test_stream_reader_name_valid_names`
Test that various valid source name patterns work correctly.

#### `test_stream_reader_name_with_different_formats`
Test that name() works with different streaming data sources.

#### `test_text`
Tests reading and writing text files, comparing Connect and standard Spark.

#### `test_writeTo_operations`
Tests `writeTo` operations (DataFrameWriterV2) with various partitioning and bucketing functions.

#### `test_write_operations`
Tests basic write operations (csv, parquet) and saving as table.

#### `test_xml`
Tests reading and writing XML files with XSD validation and various schemas, comparing Connect and standard Spark.

### Class: `SparkConnectSQLTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_connect_retry.py`

### Class: `RetryTests`

#### `test_below_limit`
Verifies that operations succeed if failures are below the retry limit.

#### `test_exceed_retries`
Verifies that exceeding retries raises an error.

#### `test_multiple_policies`
Tests that multiple retry policies can be applied correctly based on error types.

#### `test_multiple_policies_exceed`
Verifies that exceeding retries with multiple policies raises an error.

#### `test_rejected_by_policy`
Verifies that errors not covered by policy are thrown immediately.

#### `test_simple`
Tests simple retry behavior with a fixed number of retries.

#### `test_specific_exception`
Tests that only specific exceptions are retried based on policy.

#### `test_specific_exception_exceed_retries`
Verifies that exceeding retries for a specific exception raises an error.

#### `test_throw_not_retriable_error`
Verifies that non-retriable errors are thrown immediately without retry.

## File: `python/pyspark/sql/tests/connect/test_connect_session.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SparkConnectSessionTests`

#### `test_api_mode`
Tests that setting `spark.api.mode` to "connect" correctly creates a `RemoteSparkSession`.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_authentication`
Tests that invalid authentication tokens raise `SparkConnectGrpcException`.

#### `test_can_create_multiple_sessions_to_different_remotes`
Tests that creating multiple sessions to different remotes creates separate session objects, and `getOrCreate` returns the active one.

#### `test_custom_channel_builder`
Tests that a custom `ChannelBuilder` can be used to create a session.

#### `test_error_enrichment_jvm_stacktrace`
Tests that JVM stacktrace inclusion in errors can be controlled by configuration.

#### `test_error_enrichment_message`
Tests that large error messages are correctly transferred without being truncated by Netty limits when error enrichment is enabled.

#### `test_error_stack_trace`
Tests that JVM stacktrace can be obtained and controlled by configuration when error enrichment is disabled.

#### `test_get_message_parameters_without_enriched_error`
Tests that message parameters can be extracted from errors even when enrichment is disabled.

#### `test_not_hitting_netty_header_limit`
Tests that small errors do not hit Netty header limits.

#### `test_progress_handler`
Tests registering, removing, and clearing progress handlers for queries.

#### `test_reset_when_server_and_client_sessionids_mismatch`
Tests that session is recreated when client and server session IDs mismatch.

#### `test_reset_when_server_session_id_mismatch`
Tests that session is recreated when cached server session ID mismatches.

#### `test_stop_invalid_session`
Tests that stopping a session that the server has already terminated does not throw an error.

#### `test_stop_session`
Tests that operations fail with "no active session" after session is stopped.

### Class: `SparkConnectSessionWithOptionsTest`

#### `test_config`
Tests that configuration options passed during session creation are correctly set.

## File: `python/pyspark/sql/tests/connect/test_connect_stat.py`

### Class: `SparkConnectSQLTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SparkConnectStatTests`

#### `test_agg_with_avg`
Tests average aggregation and sum aggregation on grouped data, comparing Connect and standard Spark.

#### `test_agg_with_two_agg_exprs`
Tests aggregation with multiple expressions (`min` and `max`) on a table.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_describe`
Tests `describe` method with single and multiple columns, comparing Connect and standard Spark.

#### `test_drop_na`
Tests `dropna` and `na.drop` with various options (how, thresh, subset), comparing Connect and standard Spark.

#### `test_fill_na`
Tests `fillna` and `na.fill` with various types and subsets, comparing Connect and standard Spark.

#### `test_grouped_data`
Extensive tests for `groupBy`, `rollup`, `cube`, and `pivot` operations with aggregations, comparing Connect and standard Spark, including error handling.

#### `test_numeric_aggregation`
Tests numeric aggregation functions (`min`, `max`, `avg`, `mean`, `sum`) on grouped, rolled up, cubed, and pivoted data, including error handling for non-numeric columns.

#### `test_random_split`
Tests `randomSplit` with weights and seed, comparing Connect and standard Spark.

#### `test_replace`
Tests `replace` and `na.replace` with values, dicts, and subsets, comparing Connect and standard Spark, including error handling.

#### `test_stat_approx_quantile`
Tests `stat.approxQuantile` method with single and multiple columns, including error handling for invalid arguments.

#### `test_stat_corr`
Tests `stat.corr` method for Pearson correlation, including error handling for unsupported methods or invalid types.

#### `test_stat_cov`
Tests `stat.cov` method for sample covariance.

#### `test_stat_freq_items`
Tests `stat.freqItems` method with and without support fraction, including error handling.

#### `test_stat_sample_by`
Tests `stat.sampleBy` (or `sampleBy`) with fractions and seed, including error handling.

#### `test_subtract`
Tests `subtract` operation between two DataFrames.

#### `test_unpivot`
Tests `unpivot` operation on a filtered DataFrame.

## File: `python/pyspark/sql/tests/connect/test_df_debug.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `SparkConnectDataFrameDebug`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_df_debug_basics`
Tests basic DataFrame debugging by extracting the execution metrics graph and asserting that the root node is part of the graph.

#### `test_df_query_execution_metrics_to_dot`
Tests that DataFrame query execution metrics can be converted to DOT format for graph visualization and contain expected keywords like "digraph".

#### `test_df_query_execution_with_writes`
Tests that query execution information is available after a write action is performed on the DataFrame.

#### `test_df_quey_execution_empty_before_execution`
Tests that DataFrame execution information is None before any action is executed.

#### `test_query_execution_text_format`
Tests that the text representation of query execution metrics contains expected execution operators like "HashAggregate".

## File: `python/pyspark/sql/tests/connect/test_parity_catalog.py`

### Class: `CatalogParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_catalog_analyze_table`
Tests the catalog API's ability to analyze a table after creation and data insertion.

#### `test_catalog_create_and_drop_database`
Tests creating and dropping databases via the catalog API, verifying existence at each step.

#### `test_catalog_drop_table`
Tests dropping a table via the catalog API and verifying that it no longer exists.

#### `test_catalog_drop_view`
Tests dropping a view via the catalog API and verifying that it no longer exists.

#### `test_catalog_get_create_table_string`
Tests retrieving the DDL string used to create a table via the catalog API.

#### `test_catalog_get_table_properties`
Tests retrieving table properties via the catalog API for a table created with specific properties.

#### `test_catalog_list_partitions`
Tests listing partitions of a table via the catalog API.

#### `test_catalog_list_views`
Tests listing views via the catalog API and verifying that a created view is present.

#### `test_catalog_truncate_table`
Tests truncating a table (removing all rows) via the catalog API.

#### `test_current_database`
Tests getting and setting the current database via the catalog API.

#### `test_database_exists`
Tests checking if a database exists via the catalog API.

#### `test_function_exists`
Tests checking if a function exists via the catalog API.

#### `test_get_database`
Tests retrieving database metadata via the catalog API.

#### `test_get_function`
Tests retrieving function metadata via the catalog API.

#### `test_get_table`
Tests retrieving table metadata via the catalog API.

#### `test_list_columns`
Tests listing columns of a table via the catalog API.

#### `test_list_databases`
Tests listing databases via the catalog API, supporting glob patterns.

#### `test_list_functions`
Tests listing functions via the catalog API, including built-in and user-defined functions, with pattern matching support.

#### `test_list_tables`
Tests listing tables in the current database or a specified database via the catalog API, supporting patterns and verifying table types (MANAGED, TEMPORARY).

#### `test_refresh_table`
Tests refreshing a table's cached data after the underlying storage has been modified externally.

#### `test_table_cache`
Tests caching, checking cache status, un-caching, and clearing the cache for tables via the catalog API.

#### `test_table_exists`
Tests checking if a table exists via the catalog API, supporting various naming resolutions.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_parity_collection.py`

### Class: `DataFrameCollectionParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_collect_time`
Tests collecting TIME data types from a DataFrame and converting them to Pandas and Arrow, ensuring correct Python `datetime.time` objects are returned.

#### `test_to_local_iterator`
Tests iterating over DataFrame rows locally using an iterator, covering cases with empty partitions.

#### `test_to_local_iterator_not_fully_consumed`
Tests that a local iterator does not cause issues when not fully consumed.

#### `test_to_local_iterator_prefetch`
Tests the local iterator with prefetching enabled.

#### `test_to_pandas`
Tests DataFrame conversion to Pandas for various data types including nulls, floats, doubles, and strings.

#### `test_to_pandas_avoid_astype`
Tests that `toPandas` does not forcefully convert types to non-nullable types when NaN values are present, ensuring floats are used to represent nullable integers if necessary.

#### `test_to_pandas_for_array_of_struct`
Calls the superclass check for `toPandas` on an array of structs, noting that Spark Connect's implementation is based on Arrow.

#### `test_to_pandas_for_empty_df_with_nested_array_columns`
Calls the superclass check for `toPandas` on an empty DataFrame with nested array columns.

#### `test_to_pandas_from_empty_dataframe`
Calls the superclass check for `toPandas` on an empty DataFrame.

#### `test_to_pandas_from_mixed_dataframe`
Calls the superclass check for `toPandas` on a DataFrame with mixed data types.

#### `test_to_pandas_from_null_dataframe`
Calls the superclass check for `toPandas` on a DataFrame containing only nulls.

#### `test_to_pandas_on_cross_join`
Calls the superclass check for `toPandas` on the result of a cross join.

#### `test_to_pandas_required_pandas_not_found`
Tests that an appropriate ImportError is raised when trying to use `toPandas` without Pandas installed.

#### `test_to_pandas_with_duplicated_column_names`
Calls the superclass check for `toPandas` on a DataFrame with duplicate column names.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_parity_column.py`

### Class: `ColumnParityTests`

#### `test_access_column`
Tests various ways to access columns in a DataFrame (by attribute, by index, by string key) and asserts expected failures for invalid indices or types.

#### `test_alias_metadata`
Tests that setting column metadata via alias works and that empty metadata clears it.

#### `test_alias_negative`
Tests that calling alias with multiple names and metadata raises an error.

#### `test_and_in_expression`
Tests bitwise AND/OR/NOT operators on columns and asserts that Python logical operators (and, or, not) raise ValueErrors.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_bitwise_operations`
Tests bitwise operations like AND, OR, XOR, and NOT on integer columns using both column methods and functions.

#### `test_cast_negative`
Tests that casting a column to an invalid type (integer instead of DataType or string) raises a PySparkTypeError.

#### `test_cast_str_representation`
Tests the string representation of cast and try_cast operations on columns.

#### `test_col_field_ops_representation`
Tests the string representation of column field operations like getField, getItem, withField, and dropFields.

#### `test_column_accessor`
Tests accessing fields within complex types like Structs, Maps, and Arrays using various accessor methods, comparing Connect and Classic results.

#### `test_column_date_time_op`
Tests applying comparison operators on TIME columns with Python `datetime.time` literals in filter expressions.

#### `test_column_name_encoding`
Ensure that created columns has `str` type consistently.

#### `test_column_name_with_non_ascii`
Tests using non-ASCII characters in column names, verifying schema representation, dtypes, and selection.

#### `test_column_operators`
Tests a wide range of column operators (arithmetic, comparison, bitwise, string functions) and ensures that Python 'in' operator fails as expected.

#### `test_column_select`
Tests basic select operations using columns.

#### `test_drop_fields`
Tests dropping fields from a struct column.

#### `test_enum_literals`
Tests using Python Enum values as literals in DataFrame operations like selection and filtering.

#### `test_eqnullsafe_classmethod_usage`
Tests the `eqNullSafe` method as a class method on `Column`.

#### `test_expr_str_representation`
Tests the string representation of expressions, specifically a `when` condition.

#### `test_field_accessor`
Tests field accessors on array, struct, and map columns.

#### `test_getitem_column`
Tests using a column expression to index into a map.

#### `test_isinstance_dataframe`
Tests that `spark.range(1)` returns an instance of `DataFrame`.

#### `test_lit_delta_representation`
Tests the string representation of duration/timedelta literals.

#### `test_lit_time_representation`
Tests the string representation of date, timestamp, and time literals.

#### `test_over_negative`
Tests that passing an invalid type to `over` raises a `PySparkTypeError`.

#### `test_transform`
Tests the `transform` method on columns with both built-in and lambda functions.

#### `test_validate_column_types`
Calls the superclass implementation of `test_validate_column_types`.

#### `test_with_field`
Tests adding or updating fields in a struct column using `withField`.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_parity_conf.py`

### Class: `ConfParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_conf`
Tests setting, getting, unsetting, and checking modifiability of configurations via `spark.conf`.

#### `test_conf_with_python_objects`
Tests setting configurations with Python objects like booleans and integers, and asserts failures for invalid types.

#### `test_get_all`
Tests retrieving all configurations via `spark.conf.getAll` and verifies that updates are reflected.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_parity_creation.py`

### Class: `DataFrameCreationParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_check_decimal_nan`
Verifies that creating a DataFrame with `NaN` decimal value raises `PySparkValueError`.

#### `test_create_dataframe_from_array_of_long`
Tests creating a DataFrame from a Row containing a native python array of longs.

#### `test_create_dataframe_from_datetime_time`
Tests creating a DataFrame from a list of `datetime.time` objects.

#### `test_create_dataframe_from_pandas_with_day_time_interval`
Tests creating a DataFrame from a pandas DataFrame containing `timedelta` objects (DayTimeIntervalType).

#### `test_create_dataframe_from_pandas_with_dst`
Tests creating a DataFrame from a pandas DataFrame with timestamps across daylight saving time transitions.

#### `test_create_dataframe_from_pandas_with_timestamp`
Tests creating a DataFrame from a pandas DataFrame with both standard timestamps and timestamps without timezone (`TimestampNTZType`).

#### `test_create_dataframe_required_pandas_not_found`
Verifies that an appropriate `ImportError` is raised if pandas is required but not installed when creating a DataFrame.

#### `test_create_nan_decimal_dataframe`
Verifies that creating a DataFrame with a `NaN` decimal value results in a `None` value in the DataFrame.

#### `test_create_str_from_dict`
Tests creating a DataFrame with a string column from dictionary data, verifying that the dictionary is converted to its string representation.

#### `test_decimal_round`
Tests the behavior of rounding decimals when creating a DataFrame.

#### `test_empty_dataframe_with_ddl_string`
Tests creating an empty DataFrame with a schema specified as a DDL string.

#### `test_empty_dataframe_with_struct_type`
Tests creating an empty DataFrame with a schema specified as a `StructType`.

#### `test_empty_schema`
Tests creating a DataFrame with an empty schema and both empty data and a single empty row.

#### `test_from_pandas_dataframe_with_zero_columns`
SPARK-55350: Test that row count is preserved when creating DataFrame from
pandas with 0 columns but with explicit schema in Spark Connect.

#### `test_invalid_argument_create_dataframe`
Verifies that passing invalid arguments (wrong type for schema, or a DataFrame as data) to `createDataFrame` raises proper `PySparkTypeError`s.

#### `test_partial_inference_failure`
Verifies that if type inference fails for a column (e.g., all values are `None`), `PySparkValueError` is raised.

#### `test_schema_inference_from_pandas_with_dict`
Tests that python dictionaries in a pandas DataFrame are correctly inferred as maps when creating a Spark DataFrame, controlled by `spark.sql.execution.pandas.inferPandasDictAsMap`.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_parity_dataframe.py`

### Class: `DataFrameParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_cache_dataframe`
Tests caching, persisting with custom storage levels, and unpersisting a DataFrame.

#### `test_cache_table`
Tests caching and uncaching tables via the catalog, including different storage levels and error handling for non-existent tables.

#### `test_coalesce_hints_with_string_parameter`
Tests that `coalesce`, `repartition_by_range`, and `rebalance` hints are correctly added to the query plan when specified by name.

#### `test_colregex`
Verifies that passing a non-string argument to `colRegex` raises a `PySparkTypeError`.

#### `test_column_iterator`
Verifies that attempting to iterate over a Column object raises a `TypeError`.

#### `test_count_star`
Tests various forms of count star (`count(*)`, `count(col(*))`, `count(expr(*))`) both directly and in aggregations.

#### `test_create_df_with_collation`
Tests creating a DataFrame with a column that has a specific collation (`UNICODE_CI`), and verifies that distinct count honors the collation.

#### `test_dataframe_star`
Tests selecting columns from a specific DataFrame in a join using the star operator (e.g., `df1["*"]`).

#### `test_df_merge_into`
Tests `mergeInto` API for Delta-like merge operations, testing various match and not-match conditions with updates, inserts, and deletes.

#### `test_df_show`
Tests `show` method and verifies that passing incorrect parameter types raises proper `PySparkTypeError`s.

#### `test_drop`
Tests that dropping columns specified as strings or Column objects correctly updates the plan.

#### `test_drop_col_from_different_dataframe`
Tests that dropping a column derived from a different DataFrame correctly identifies and drops the semantically equivalent column in the target DataFrame.

#### `test_drop_column_name_with_dot`
Tests that dropping columns with dots in their names works correctly.

#### `test_drop_duplicates`
Tests `dropDuplicates` (or `distinct`), including subset specification and error handling for invalid arguments.

#### `test_drop_duplicates_with_ambiguous_reference`
Tests that `dropDuplicates` works correctly even when the provided subset contains references that might be ambiguous across joins.

#### `test_drop_empty_column`
Tests that calling `drop` without arguments or with an empty list does not drop any columns.

#### `test_drop_join`
Tests that dropping a join key from a joined DataFrame drops only the column from the specified side, preserving the other.

#### `test_drop_notexistent_col`
Tests that attempting to drop a column that doesn't exist leaves the DataFrame unchanged.

#### `test_duplicate_field_names`
Tests that DataFrames can be created with schemas containing duplicate field names within structs.

#### `test_duplicated_column_names`
Tests behavior of DataFrame with duplicate top-level column names, verifying string representation and that accessing ambiguous columns raises `AnalysisException`.

#### `test_extended_hint_types`
Tests that hint parameters can be strings, floats, ints, Columns, and lists thereof, and that passing invalid types raises `PySparkTypeError`.

#### `test_generic_hints`
Tests basic hint usage (like broadcast) and verifies that hints are reflected in the query plan.

#### `test_help_command`
Calls a helper to verify the behavior of help command.

#### `test_input_files`
Tests that `inputFiles` returns the list of files that make up the DataFrame's source.

#### `test_invalid_join_method`
Verifies that passing an invalid join type to `join` raises an `AnalysisException`.

#### `test_isinstance_dataframe`
Verifies that a DataFrame returned by `spark.range` is an instance of `DataFrame`.

#### `test_join_without_on`
Verifies that joins without conditions raise an exception unless cross joins are explicitly enabled.

#### `test_lateral_column_alias`
Tests that column aliases defined in a `select` can be used by subsequent expressions in the same select (lateral column alias), and that they can be resolved properly in joins.

#### `test_local_checkpoint_dataframe`
Verifies that `localCheckpoint` is reflected as an `ExistingRDD` plan node.

#### `test_local_checkpoint_dataframe_with_storage_level`
Tests calling `localCheckpoint` with eager execution and a specific storage level.

#### `test_metadata_column`
Tests that `metadataColumn` returns expected values on a partitioned table with metadata columns.

#### `test_ordering_of_with_columns_renamed`
Tests that `withColumnsRenamed` respects the order of the dictionary for sequential renames.

#### `test_pandas_api`
Tests that `pandas_api()` returns a pandas-on-Spark DataFrame with equivalent content to the original DataFrame.

#### `test_print_schema`
Verifies that the string representation of the schema matches the expected tree structure.

#### `test_query_execution_unsupported_in_classic`
*No description available.*

#### `test_range`
Tests that `range` parameters are correctly translated to the proto plan.

#### `test_repr_behaviors`
Tests DataFrame string and HTML representations under different configurations for eager evaluation and truncation.

#### `test_require_cross`
Verifies that cross joins without conditions raise an exception unless cross joins are explicitly enabled.

#### `test_same_semantics_error`
Verifies that passing a non-DataFrame to `sameSemantics` raises a `PySparkTypeError`.

#### `test_sample`
Tests that `sample` parameters are correctly translated to the proto plan.

#### `test_sample_with_random_seed`
Verifies that sampling without a seed returns a consistent number of rows across repeated counts in a short interval (likely cached or small data).

#### `test_select_join_keys`
Verifies that selecting the join key column from the resulting DataFrame after a join doesn't fail for various join types.

#### `test_self_join`
Tests that joining a DataFrame with a filtered version of itself works and properly resolves column references in Spark Connect.

#### `test_self_join_II`
Tests joining a DataFrame with a projected version of itself.

#### `test_self_join_III`
Tests joining a DataFrame with a unioned version of itself.

#### `test_self_join_IV`
Tests right join of a DataFrame with a unioned version of itself that has different projected values.

#### `test_socket_leak`
Verifies that `collect` and `toLocalIterator` do not cause `ResourceWarning` for socket leaks.

#### `test_table`
Verifies that passing `None` as table name to `spark.table` raises a `PySparkTypeError`.

#### `test_to`
Tests that the `to` method correctly updates the plan schema to match the requested schema in Spark Connect.

#### `test_toDF_with_schema_string`
*No description available.*

#### `test_toDF_with_string`
Tests `toDF` with new column names specified as arguments, and error handling for invalid types.

#### `test_to_json`
*No description available.*

#### `test_transpose`
Tests the `transpose` method on a DataFrame, verifying results with default and specified index columns, respecting row limit configuration, and error handling for invalid index column types or if columns cannot be cast to a common type.

#### `test_transpose_with_invalid_index_columns`
Verifies that using an expression as index column for transpose raises an `AnalysisException`.

#### `test_union_classmethod_usage`
Tests that `DataFrame.union` class method can be used directly on DataFrames.

#### `test_where`
Verifies that passing a non-column, non-string argument to `where` raises a `PySparkTypeError`.

#### `test_with_column_and_generator`
Tests that generator functions like `explode` can be used directly within `withColumn`.

#### `test_with_column_with_existing_name`
Verifies that `withColumn` replacing an existing column with its own value works correctly.

#### `test_with_columns`
Adding/replacing columns via dictionary in `withColumns`.

#### `test_with_columns_renamed`
Tests renaming multiple columns at once using a dictionary in `withColumnsRenamed`.

#### `test_with_columns_renamed_with_duplicated_names`
Verifies that renaming columns with duplicate names behaves correctly and consistently between withColumnRenamed and withColumnsRenamed.

#### `test_zip_with_index`
Tests adding an index column to a DataFrame, verifying column names and error conditions for duplicate names.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_parity_dataframe_query_context.py`

### Class: `DataFrameQueryContextParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_dataframe_query_context`
Verifies that arithmetic and comparison operations on DataFrames produce correct query context in error messages under ANSI mode.

#### `test_dataframe_query_context_col`
Verifies that unresolved column errors in select with col() produce the expected query context.

#### `test_query_context_complex`
Tests both SQL and DataFrame query contexts for divide-by-zero errors.

#### `test_sql_query_context`
Tests that SQL queries produce correct query context for runtime errors and no context for analysis errors.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_parity_datasources.py`

### Class: `DataSourcesParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_checking_csv_header`
Verifies that an exception is raised when a CSV header does not match the provided schema.

#### `test_csv_sampling_ratio`
Verifies CSV sampling ratio functionality by calling the superclass test.

#### `test_custom_data_source`
Tests that unimplemented methods in a custom data source raise PySparkNotImplementedError.

#### `test_encoding_json`
Verifies reading a multi-line JSON file with UTF-16LE encoding.

#### `test_ignore_column_of_all_nulls`
Verifies that columns with only null values can be dropped when reading JSON data.

#### `test_ignorewhitespace_csv`
Verifies that leading and trailing whitespaces are preserved in CSV when specified.

#### `test_input_partition`
Tests the string representation of InputPartition and its subclasses.

#### `test_jdbc`
Tests basic JDBC read and write operations, including with partitions and predicates.

#### `test_jdbc_format`
Tests JDBC read and write operations using the generic format API.

#### `test_json_sampling_ratio`
Verifies JSON sampling ratio functionality by calling the superclass test.

#### `test_json_with_dataframe_input`
Verifies reading JSON data from an existing DataFrame containing JSON strings.

#### `test_json_with_dataframe_input_and_schema`
Verifies reading JSON data from a DataFrame with a user-specified schema.

#### `test_json_with_dataframe_input_multiple_columns`
Verifies that reading JSON from a multi-column DataFrame only uses the first string column.

#### `test_json_with_dataframe_input_non_string_column`
Verifies that an exception is raised when attempting to read JSON from a non-string DataFrame column.

#### `test_json_with_dataframe_input_zero_columns`
Verifies that an exception is raised when attempting to read JSON from an empty-schema DataFrame.

#### `test_linesep_json`
Verifies custom line separator support for JSON read and write operations.

#### `test_linesep_text`
Verifies custom line separator support for text read and write operations.

#### `test_multiline_csv`
Verifies reading CSV files where records span multiple lines.

#### `test_multiline_json`
Verifies reading JSON files where objects span multiple lines.

#### `test_read_multiple_orc_file`
Verifies reading multiple ORC files by passing a list of paths.

#### `test_read_text_file_list`
Verifies reading multiple text files by passing a list of paths.

#### `test_xml`
Verifies XML read and write operations, including XSD validation and schema application.

#### `test_xml_sampling_ratio`
Verifies XML sampling ratio functionality by calling the superclass test.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_parity_errors.py`

### Class: `ErrorsParityTests`

#### `test_arithmetic_exception`
Verifies that a divide-by-zero error in SQL raises an ArithmeticException.

#### `test_array_index_out_of_bounds_exception`
Verifies that accessing an out-of-bounds array index in SQL raises an ArrayIndexOutOfBoundsException.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_date_time_exception`
Verifies that an invalid date/time format in SQL raises a DateTimeException.

#### `test_number_format_exception`
Verifies that an invalid string-to-number cast in SQL raises a NumberFormatException.

#### `test_spark_runtime_exception`
Verifies that an invalid string-to-boolean cast in SQL raises a SparkRuntimeException.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_parity_frame_plot.py`

### Class: `FramePlotParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_backend`
Verifies that "plotly" is the default backend and that unsupported backends raise an error.

#### `test_sampled_plot_with_max_rows`
Verifies that large DataFrames are sampled appropriately for plotting.

#### `test_topn_max_rows`
Verifies that top-N plotting respects the maximum rows configuration.

#### `test_unsupported_plot_kind`
Verifies that an exception is raised when an unsupported plot kind is requested.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_parity_frame_plot_plotly.py`

### Class: `FramePlotPlotlyParityTests`

#### `test_area_plot`
Verifies area plot generation using the Plotly backend for single and multiple columns.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_bar_plot`
Verifies bar plot generation using the Plotly backend for single and multiple columns.

#### `test_barh_plot`
Verifies horizontal bar plot generation using the Plotly backend.

#### `test_box_plot`
Verifies box plot generation and that unsupported Plotly parameters raise errors.

#### `test_hist_plot`
Verifies histogram generation using the Plotly backend.

#### `test_kde_plot`
Verifies Kernel Density Estimation (KDE) plot generation using the Plotly backend.

#### `test_line_plot`
Verifies line plot generation using the Plotly backend.

#### `test_pie_plot`
Verifies pie plot generation, requiring subplots or single columns, and failing on non-numeric types.

#### `test_process_column_param_errors`
Verifies that selecting non-existent or non-numeric columns for plotting raises appropriate type errors.

#### `test_scatter_plot`
Verifies scatter plot generation using the Plotly backend.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_parity_functions.py`

### Class: `FunctionsParityTests`

#### `test_add_months_function`
Verifies the add_months function with both column and literal month offsets.

#### `test_approxQuantile`
Verifies approxQuantile on single and multiple columns, ensuring expected list return types.

#### `test_array_contains_function`
Verifies the array_contains function returns correct boolean results.

#### `test_array_repeat`
Verifies array_repeat using both literal counts and column references.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_assert_true`
Verifies that assert_true raises a SparkRuntimeException on failure.

#### `test_avro_type_check`
Verifies that from_avro and to_avro raise PySparkTypeError on invalid argument types.

#### `test_basic_functions`
Verifies basic function parity by calling the superclass test.

#### `test_between_function`
Verifies that the between filter works correctly with column values.

#### `test_binary_math_function`
Verifies results of atan2, hypot, pow, and pmod binary math functions against expected values.

#### `test_bit_length_function`
Verifies bit_length function on both regular and multi-byte character strings.

#### `test_bool_ndarray`
Verifies that NumPy boolean arrays are correctly translated to Spark boolean arrays in lit().

#### `test_bucket`
Verifies that bucket raises a PySparkTypeError when non-integer bucket counts are provided.

#### `test_collation`
Verifies the application of collation strings using collate and reading them back with collation.

#### `test_collect_functions`
Verifies collect_set (unique values) and collect_list (all values) aggregation functions.

#### `test_convert_timezone`
Verifies timezone conversion results in expected datetime objects.

#### `test_corr`
Verifies Pearson correlation coefficient between two DataFrame columns.

#### `test_cov`
Verifies sample covariance calculation and checks for type errors on invalid column name inputs.

#### `test_crosstab`
Verifies plan generation for crosstab on DataFrames.

#### `test_current_time`
Verifies that current_time returns a datetime.time object with proper precision suffix in schema.

#### `test_current_timestamp`
Verifies that current_timestamp and now functions both return proper datetime values.

#### `test_current_user`
Verifies that current_user, user, and session_user string return types map properly in schemas.

#### `test_date_add_function`
Verifies date_add function with both column and literal day offsets.

#### `test_date_sub_function`
Verifies date_sub function with both column and literal day offsets.

#### `test_datetime_functions`
Verifies converting a string column to a date column using to_date.

#### `test_dayname`
Verifies that the dayname function returns the short string name of the day (e.g., "Mon").

#### `test_dayofweek`
Verifies that the dayofweek function returns the correct numeric day of the week.

#### `test_empty_ndarray`
Verifies that empty NumPy arrays of various types are mapped to the correct Spark array types in lit().

#### `test_enum_literals`
Verifies that Python enums are correctly handled when used as literals in Spark expressions.

#### `test_explode`
Tests array and map unnesting behaviors using normal and outer variants of explode and posexplode.

#### `test_expr`
Verifies parsing raw SQL expressions via the expr() function.

#### `test_first_last_ignorenulls`
Verifies that first and last aggregations correctly respect the ignoreNulls flag.

#### `test_from_csv`
Verifies that from_csv raises a PySparkTypeError when a non-string/column schema is passed.

#### `test_from_xml`
Verifies that from_xml raises a PySparkTypeError when an invalid schema argument is passed.

#### `test_function_parity`
Verifies function parity by calling the superclass test.

#### `test_functions_broadcast`
Verifies that the broadcast() hint forces broadcast joins and doesn't crash on standard select explain plans.

#### `test_greatest`
Verifies that the greatest function raises an error if passed fewer than two arguments.

#### `test_higher_order_function_failures`
Verifies that invalid lambda function signatures passed to higher-order functions like transform raise appropriate errors.

#### `test_hour`
Verifies that the hour function extracts the hour correctly from a datetime.time object.

#### `test_inline`
Tests array of struct unnesting using inline and inline_outer.

#### `test_input_file_name_reset_for_rdd`
Verifies input_file_name reset behavior for RDDs by calling the superclass test.

#### `test_input_file_name_udf`
Verifies that input_file_name() returns the proper file name even when combined with a Python UDF.

#### `test_inverse_trig_functions`
Verifies that inverse hyperbolic trig functions correctly invert hyperbolic trig functions.

#### `test_json_tuple_empty_fields`
Verifies that json_tuple raises a PySparkValueError if no fields are requested from the JSON string.

#### `test_kll_merge_agg_bigint`
Test kll_merge_agg_bigint function

#### `test_kll_merge_agg_double`
Test kll_merge_agg_double function

#### `test_kll_merge_agg_float`
Test kll_merge_agg_float function

#### `test_kll_merge_agg_with_different_k`
Test kll_merge_agg with different k values

#### `test_kll_merge_agg_with_nulls`
Test kll_merge_agg with null values

#### `test_kll_sketch_agg_bigint`
Test kll_sketch_agg_bigint function

#### `test_kll_sketch_agg_double`
Test kll_sketch_agg_double function

#### `test_kll_sketch_agg_float`
Test kll_sketch_agg_float function

#### `test_kll_sketch_double_variants`
Test all double variant functions

#### `test_kll_sketch_float_variants`
Test all float variant functions

#### `test_kll_sketch_get_n_bigint`
Test kll_sketch_get_n_bigint function

#### `test_kll_sketch_get_quantile_bigint`
Test kll_sketch_get_quantile_bigint function

#### `test_kll_sketch_get_quantile_bigint_array`
Test kll_sketch_get_quantile_bigint with array of ranks

#### `test_kll_sketch_get_rank_bigint`
Test kll_sketch_get_rank_bigint function

#### `test_kll_sketch_merge_bigint`
Test kll_sketch_merge_bigint function

#### `test_kll_sketch_to_string_bigint`
Test kll_sketch_to_string_bigint function

#### `test_kll_sketch_with_nulls`
Test KLL sketch with null values

#### `test_least`
Verifies least function on multi-columns and checks for error on fewer than two columns.

#### `test_levenshtein_function`
Verifies Levenshtein distance between strings with and without a maximum threshold limit.

#### `test_listagg_distinct_functions`
Verifies string concatenation aggregations with distinct values and custom separators.

#### `test_listagg_functions`
Verifies string concatenation aggregations with all values and custom separators.

#### `test_lit_day_time_interval`
Verifies that Python timedelta objects are supported in Spark's literal lit() constructor.

#### `test_lit_list`
Verifies that Python lists are correctly handled as literal array values by lit().

#### `test_lit_np_scalar`
Verifies that various NumPy scalar types are correctly translated to proper Spark literal types by lit().

#### `test_lit_time`
Verifies that Python datetime.time objects are supported in Spark's literal lit() constructor.

#### `test_make_date`
Verifies make_date function by creating a date from integer year, month, and day columns.

#### `test_make_time`
Verifies make_time function by creating a time from integer hour, minute, and second columns.

#### `test_make_timestamp`
Comprehensive test cases for make_timestamp with various arguments and edge cases.

#### `test_make_timestamp_ntz`
Comprehensive test cases for make_timestamp_ntz with various arguments and edge cases.

#### `test_map_concat`
Verifies map_concat by combining two map columns into a single map.

#### `test_map_functions`
Verifies functionality of map_from_arrays, map_contains_key, map_keys, map_values, map_entries, and map_from_entries.

#### `test_math_functions`
Runs a comprehensive parity test for dozens of math functions against both active and traditional execution modes.

#### `test_max_by_min_by_with_k`
Test max_by and min_by aggregate functions with k parameter

#### `test_median`
Verifies that the median() column function is correctly constructed.

#### `test_minute`
Verifies that the minute function extracts the minute correctly from a datetime.time object.

#### `test_monthname`
Verifies that the monthname function returns the short string name of the month (e.g., "Nov").

#### `test_ndarray_input`
Verifies that NumPy arrays map to Spark arrays and that unsupported types like uint64 raise an error in lit().

#### `test_nested_higher_order_function`
Verifies variable resolution in nested high order functions, like transform() within a transform().

#### `test_non_deterministic_with_seed`
Verifies that duplicate non-deterministic function calls in the same query return identical results.

#### `test_np_scalar_input`
Verifies that NumPy numeric scalars work properly in array functions like array_contains and array_position.

#### `test_nth_value`
Verifies the nth_value window function with and without the ignoreNulls behavior.

#### `test_nullifzero_zeroifnull`
Verifies functions that map zeroes to nulls and nulls to zeroes.

#### `test_octet_length_function`
Verifies octet_length function on regular and multi-byte character strings.

#### `test_overlay`
Verifies that overlay replaces substring parts, and checks for error conditions on non-integer bounds.

#### `test_parse_json`
Verifies creating JSON variants using parse_json and serializing back to string via to_json.

#### `test_percentile`
Verifies that the percentile aggregation is correctly parsed for single percentiles, arrays, and scale factors.

#### `test_percentile_approx`
Verifies that the percentile_approx aggregation is correctly parsed for single and multiple percentiles with accuracy settings.

#### `test_raise_error`
Verifies that the raise_error function produces a SparkRuntimeException when invoked in evaluation.

#### `test_rand_functions`
Verifies that random functions return values in expected ranges and produce identical sequences when passed a seed.

#### `test_randstr_uniform`
Verifies that the randstr and uniform random generation functions return data fitting length or bound conditions.

#### `test_reciprocal_trig_functions`
Verifies accuracy for reciprocal trig functions sec, csc, and cot against standard mathematical definitions.

#### `test_regexp_replace`
Verifies that the regexp_replace string function operates properly using both literal and column patterns.

#### `test_sampleby`
Verifies stratified sampling via sampleBy and checks for proper raised errors on invalid input types.

#### `test_schema_of_csv`
Verifies that passing non-string/column types to schema_of_csv results in appropriate type errors.

#### `test_schema_of_json`
Verifies that passing non-string/column types to schema_of_json results in appropriate type errors.

#### `test_schema_of_xml`
Verifies that passing non-string/column types to schema_of_xml results in appropriate type errors.

#### `test_second`
Verifies that the second function extracts the second correctly from a datetime.time object.

#### `test_session_window`
Verifies that non-string or non-column gap durations raise type errors in the session_window definition.

#### `test_shiftleft`
Tests that `shiftLeft` and `shiftleft` (alias) produce the same result.

#### `test_shiftright`
Tests that `shiftRight` and `shiftright` (alias) produce the same result.

#### `test_shiftrightunsigned`
Tests that `shiftRightUnsigned` and `shiftrightunsigned` (alias) produce the same result.

#### `test_slice`
Tests `slice` function with constant and column arguments for start and length.

#### `test_sort_with_nulls_order`
Tests sorting functions (`asc`, `asc_nulls_first`, `asc_nulls_last`, `desc`, `desc_nulls_first`, `desc_nulls_last`) on DataFrame with null values.

#### `test_sorting_functions_with_column`
Tests that sorting functions accept Column objects and return Column objects with correct string representation.

#### `test_st_asbinary`
Tests `st_asbinary` function to convert geography/geometry to WKB.

#### `test_st_geogfromwkb`
Tests `st_geogfromwkb` function to construct geography from WKB, including error handling for invalid WKB.

#### `test_st_geomfromwkb`
Tests `st_geomfromwkb` function to construct geometry from WKB, with and without SRID, including error handling for invalid WKB.

#### `test_st_setsrid`
Tests `st_setsrid` function to set SRID on geography/geometry.

#### `test_st_srid`
Tests `st_srid` function to get SRID from geography/geometry.

#### `test_str_ndarray`
Tests that numpy string arrays are correctly converted to Spark array of strings when used as literals.

#### `test_string_functions`
Tests various string functions (`upper`, `lower`, etc.) and error handling for invalid arguments to `substr`.

#### `test_string_validation`
Tests UTF-8 validation functions: `is_valid_utf8`, `make_valid_utf8`, `validate_utf8`, `try_validate_utf8`.

#### `test_sum_distinct`
Tests that `sum_distinct` and `sumDistinct` (alias) produce the same result.

#### `test_time_diff`
Tests `time_diff` function to get difference between two times in specified unit.

#### `test_time_trunc`
Tests `time_trunc` function to truncate time to specified unit.

#### `test_to_time`
Tests `to_time` function to parse string to time, with and without format.

#### `test_to_timestamp_ltz`
Tests `to_timestamp_ltz` function to parse string to timestamp with local timezone.

#### `test_to_timestamp_ntz`
Tests `to_timestamp_ntz` function to parse string to timestamp without timezone.

#### `test_to_variant_object`
Tests `to_variant_object` function to convert struct to variant.

#### `test_try_datetime_functions`
Tests `try_to_date` function.

#### `test_try_make_interval`
Tests `try_make_interval` function, verifying it returns null on overflow.

#### `test_try_make_timestamp`
Comprehensive test cases for try_make_timestamp with various arguments.

#### `test_try_make_timestamp_ltz`
Tests `try_make_timestamp_ltz` function, verifying it returns null for invalid inputs.

#### `test_try_make_timestamp_ntz`
Test cases for try_make_timestamp_ntz with 6-parameter and date/time forms.

#### `test_try_parse_json`
Tests `try_parse_json` function, verifying it returns null for invalid JSON strings.

#### `test_try_parse_url`
Tests `try_parse_url` function, verifying it returns null for invalid URLs.

#### `test_try_to_time`
Tests `try_to_time` function, verifying it returns null for malformed time strings.

#### `test_tuple_difference_double_basic`
Test tuple_difference_double basic functionality

#### `test_tuple_difference_integer_basic`
Test tuple_difference_integer basic functionality

#### `test_tuple_difference_theta_double_basic`
Test tuple_difference_theta_double basic functionality

#### `test_tuple_difference_theta_integer_basic`
Test tuple_difference_theta_integer basic functionality

#### `test_tuple_intersection_agg_double_basic`
Test tuple_intersection_agg_double basic functionality

#### `test_tuple_intersection_agg_integer_basic`
Test tuple_intersection_agg_integer basic functionality

#### `test_tuple_intersection_double_basic`
Test tuple_intersection_double basic functionality

#### `test_tuple_intersection_integer_basic`
Test tuple_intersection_integer basic functionality

#### `test_tuple_intersection_theta_double_basic`
Test tuple_intersection_theta_double basic functionality

#### `test_tuple_intersection_theta_integer_basic`
Test tuple_intersection_theta_integer basic functionality

#### `test_tuple_sketch_agg_double_basic`
Test tuple_sketch_agg_double basic functionality

#### `test_tuple_sketch_agg_integer_basic`
Test tuple_sketch_agg_integer basic functionality

#### `test_tuple_sketch_comprehensive_double`
Test tuple_sketch_agg + operations + estimate comprehensive test - double

#### `test_tuple_sketch_comprehensive_integer`
Test tuple_sketch_agg + operations + estimate comprehensive test - integer

#### `test_tuple_sketch_estimate_and_summary_double`
Test tuple_sketch_estimate and summary functions - double

#### `test_tuple_sketch_estimate_and_summary_integer`
Test tuple_sketch_estimate and summary functions - integer

#### `test_tuple_sketch_with_nulls`
Test tuple sketch with null values

#### `test_tuple_union_agg_double_basic`
Test tuple_union_agg_double basic functionality

#### `test_tuple_union_agg_integer_basic`
Test tuple_union_agg_integer basic functionality

#### `test_tuple_union_double_basic`
Test tuple_union_double basic functionality

#### `test_tuple_union_integer_basic`
Test tuple_union_integer basic functionality

#### `test_tuple_union_theta_double_basic`
Test tuple_union_theta_double basic functionality

#### `test_tuple_union_theta_integer_basic`
Test tuple_union_theta_integer basic functionality

#### `test_variant_expressions`
*No description available.*

#### `test_version`
*No description available.*

#### `test_when`
*No description available.*

#### `test_wildcard_import`
*No description available.*

#### `test_window`
*No description available.*

#### `test_window_functions`
*No description available.*

#### `test_window_functions_cumulative_sum`
*No description available.*

#### `test_window_functions_moving_average`
*No description available.*

#### `test_window_functions_without_partitionBy`
*No description available.*

#### `test_window_time`
*No description available.*

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_parity_geographytype.py`

### Class: `GeographyTypeParityTest`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_geographytype_any_specifier`
Test that GeographyType is constructed correctly with ANY specifier for mixed SRID.

#### `test_geographytype_different_srid_values`
Test that two GeographyTypes with specified SRIDs have different SRID values.

#### `test_geographytype_from_invalid_algorithm`
Test that GeographyType construction fails when an invalid CRS is specified.

#### `test_geographytype_from_invalid_crs`
Test that GeographyType construction fails when an invalid CRS is specified.

#### `test_geographytype_from_valid_crs_and_algorithm`
Test that GeographyType construction passes when valid CRS & ALG are specified.

#### `test_geographytype_same_srid_values`
Test that two GeographyTypes with specified SRIDs have the same SRID values.

#### `test_geographytype_specified_invalid_srid`
Test that the correct error is returned when an invalid SRID value is specified.

#### `test_geographytype_specified_valid_srid`
Test that GeographyType is constructed correctly when a valid SRID is specified.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_parity_geometrytype.py`

### Class: `GeometryTypeParityTest`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_geometrytype_any_specifier`
Test that GeometryType is constructed correctly with ANY specifier for mixed SRID.

#### `test_geometrytype_different_srid_values`
Test that two GeometryTypes with specified SRIDs have different SRID values.

#### `test_geometrytype_from_invalid_crs`
Test that GeometryType construction fails when an invalid CRS is specified.

#### `test_geometrytype_from_valid_crs`
Test that GeometryType construction passes when a valid CRS is specified.

#### `test_geometrytype_same_srid_values`
Test that two GeometryTypes with specified SRIDs have the same SRID values.

#### `test_geometrytype_specified_invalid_srid`
Test that the correct error is returned when an invalid SRID value is specified.

#### `test_geometrytype_specified_valid_srid`
Test that GeometryType is constructed correctly when a valid SRID is specified.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_parity_group.py`

### Class: `GroupParityTests`

#### `test_agg_func`
Tests basic aggregation functions (`max`, `min`, `sum`, `count`, `mean`) on a grouped DataFrame, and pivot operation.

#### `test_aggregator`
Tests `agg` method with dictionary mapping and function calls (`first`, `last`, `approx_count_distinct`, `countDistinct`) on a grouped DataFrame.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_group_by_ordinal`
Tests `groupBy` with ordinal column references (integers) and verifies error handling for out-of-range ordinals.

#### `test_numeric_agg_with_nest_type`
Tests aggregation (`max`) on a nested struct field after grouping.

#### `test_order_by_ordinal`
Tests `orderBy` with ordinal column references (negative integers) and verifies error handling for out-of-range ordinals.

#### `test_pivot_exceed_max_values`
Verifies that pivoting with too many distinct values raises `AnalysisException`.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_parity_job_cancellation.py`

### Class: `JobCancellationParityTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_inheritable_tags`
Tests that tags are inherited by threads created with `InheritableThread` or decorated with `inheritable_thread_target`.

#### `test_inheritable_tags_with_deco`
Tests that tags are inherited by threads using `inheritable_thread_target` decorator.

#### `test_interrupt_all`
Tests `interrupt_all` method on `SparkConnectClient`.

#### `test_interrupt_tag`
Tests `interruptTag` to cancel jobs with a specific tag.

#### `test_tags`
Tests basic tag management: `addTag`, `getTags`, `removeTag`, `clearTags`.

#### `test_tags_multithread`
Tests that tags are thread-local and not shared between independent threads.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_parity_memory_profiler.py`

### Class: `MemoryProfilerParityTests`

#### `test_memory_profiler_udf_multiple_actions`
Verifies that the memory profiler correctly captures profiles for a UDF when it is used across multiple actions. It checks that without plan caching, different UDF IDs are generated for each action and that the expected profile output is generated.

#### `test_memory_profiler_aggregate_in_pandas`
Verifies memory profiling for pandas UDF aggregation.

#### `test_memory_profiler_clear`
Verifies clearing of memory profile results.

#### `test_memory_profiler_cogroup_apply_in_arrow`
Verifies memory profiling for cogroup().applyInArrow().

#### `test_memory_profiler_cogroup_apply_in_pandas`
Verifies memory profiling for cogroup().applyInPandas().

#### `test_memory_profiler_different_function`
Verifies memory profiling when a UDF calls another function.

#### `test_memory_profiler_group_apply_in_arrow`
Verifies memory profiling for groupBy().applyInArrow().

#### `test_memory_profiler_group_apply_in_pandas`
Verifies memory profiling for groupBy().applyInPandas().

#### `test_memory_profiler_map_in_pandas`
Verifies memory profiling for mapInPandas().

#### `test_memory_profiler_pandas_udf`
Verifies memory profiling for standard pandas UDFs.

#### `test_memory_profiler_pandas_udf_iterator`
Verifies memory profiling for pandas UDFs that use iterators.

#### `test_memory_profiler_pandas_udf_window`
Verifies memory profiling for pandas UDFs used in window functions.

#### `test_memory_profiler_udf`
Verifies memory profiling for standard Python UDFs and dumping results.

#### `test_memory_profiler_udf_registered`
Verifies memory profiling for registered SQL UDFs.

#### `test_memory_profiler_udf_with_arrow`
Verifies memory profiling for Python UDFs with Arrow execution enabled.

#### `test_profilers_clear`
Verifies clearing both memory and performance profilers.

### Class: `MemoryProfilerWithoutPlanCacheParityTests`

#### `test_memory_profiler_udf_multiple_actions`
Same as above, but in the context of tests without plan cache.

#### `test_memory_profiler_aggregate_in_pandas`
Verifies memory profiling for pandas UDF aggregation.

#### `test_memory_profiler_clear`
Verifies clearing of memory profile results.

#### `test_memory_profiler_cogroup_apply_in_arrow`
Verifies memory profiling for cogroup().applyInArrow().

#### `test_memory_profiler_cogroup_apply_in_pandas`
Verifies memory profiling for cogroup().applyInPandas().

#### `test_memory_profiler_different_function`
Verifies memory profiling when a UDF calls another function.

#### `test_memory_profiler_group_apply_in_arrow`
Verifies memory profiling for groupBy().applyInArrow().

#### `test_memory_profiler_group_apply_in_pandas`
Verifies memory profiling for groupBy().applyInPandas().

#### `test_memory_profiler_map_in_pandas`
Verifies memory profiling for mapInPandas().

#### `test_memory_profiler_pandas_udf`
Verifies memory profiling for standard pandas UDFs.

#### `test_memory_profiler_pandas_udf_iterator`
Verifies memory profiling for pandas UDFs that use iterators.

#### `test_memory_profiler_pandas_udf_window`
Verifies memory profiling for pandas UDFs used in window functions.

#### `test_memory_profiler_udf`
Verifies memory profiling for standard Python UDFs and dumping results.

#### `test_memory_profiler_udf_registered`
Verifies memory profiling for registered SQL UDFs.

#### `test_memory_profiler_udf_with_arrow`
Verifies memory profiling for Python UDFs with Arrow execution enabled.

#### `test_profilers_clear`
Verifies clearing both memory and performance profilers.

## File: `python/pyspark/sql/tests/connect/test_parity_observation.py`

### Class: `DataFrameObservationParityTests`

#### `test_observation_errors_propagated_to_client`
Tests that errors occurring during data observation (e.g., calling `F.raise_error`) are correctly propagated back to the client as a `PySparkException`.

#### `test_observe`
Tests the `DataFrame.observe()` method. It creates a plan with observations (min, max, sum) and verifies that the generated protobuf plan correctly reflects these metrics and functions. It also tests using a custom `Observation` object.

#### `test_observe_lateral_join`
Tests self-joining a DataFrame that has been observed with a lateral join. It verifies that the join produces expected results and that the observation metrics can still be retrieved. It also tests error conditions for reusing observations and duplicate metric names.

#### `test_observe_on_commands`
Tests that `observe` works correctly with various DataFrame commands like `collect`, `show`, `save`, and `create` (via `writeTo`). It runs these with plan caching enabled and disabled, and with/without a select star.

#### `test_observe_self_join`
Tests self-joining an observed DataFrame using an inner join. Verifies that the result matches expected IDs and that observation metrics are correctly collected. Also checks for expected errors when reusing observations inappropriately.

#### `test_observe_self_join_union`
Tests the union of two filtered views of the same observed DataFrame, verifying that the full range of data is preserved and the observation reflects the total count.

#### `test_observe_str`
Tests streaming query observation using a string metric name. It adds a `StreamingQueryListener`, runs a streaming query on rate source with observation, and waits for observed metrics to appear in query progress events.

#### `test_observe_with_array_type`
Verifies that DataFrame observation correctly handles and returns an array type metric (specifically an array containing a count).

#### `test_observe_with_map_type`
Verifies that DataFrame observation correctly handles and returns a map type metric (specifically a map containing a literal string key and a count value).

#### `test_observe_with_same_name_on_different_dataframe`
Verifies that using the same name for `Observation` instances on different DataFrames results in independent and correct metric collection for each DataFrame.

#### `test_observe_with_struct_type`
Verifies that DataFrame observation correctly handles and returns a struct type metric containing both count and max values.

## File: `python/pyspark/sql/tests/connect/test_parity_python_datasource.py`

### Class: `PythonDataSourceParityTests`

#### `test_arrow_batch_data_source`
Tests a custom data source that yields Arrow Record Batches. It verifies that data is read correctly and that errors are raised for schema mismatches between expected schema and yielded batch schema (both column count/names and types).

#### `test_arrow_batch_sink`
Tests writing data to a custom sink that uses `DataSourceArrowWriter`. It writes DataFrame rows to JSON files and asserts that the re-read data matches original input DataFrame.

#### `test_basic_data_source_class`
Tests basic properties and expected `NotImplementedError`s for base class instances of `DataSource` with specified options.

#### `test_basic_data_source_reader_class`
Tests simple instantiation and execution of a minimal reader subclassing `DataSourceReader` yielding a single tuple.

#### `test_case_insensitive_dict`
Verifies correctness of standard operations (read, set, delete, update, copy) on custom dictionary ensuring keys are handled in a case-insensitive manner.

#### `test_custom_json_data_source_abort`
Asserts appropriate abort behavior and error reporting via specific file output when writing large range using failing dummy JSON custom data source.

#### `test_custom_json_data_source_commit`
Tests correct commit behavior of success file generation holding expected count after appending small data ranges using functional dummy JSON custom data source.

#### `test_custom_json_data_source_read`
Verifies reading from a custom JSON data source.

#### `test_custom_json_data_source_write`
Verifies writing to a custom JSON data source.

#### `test_data_source_memory_profiler`
Verifies memory profiling for custom data sources.

#### `test_data_source_perf_profiler`
Verifies performance profiling for custom data sources.

#### `test_data_source_read_cast_output_schema`
Verifies data source read with output schema casting.

#### `test_data_source_read_output_empty_iter`
Verifies data source read returning an empty iterator.

#### `test_data_source_read_output_list`
Verifies data source read returning lists.

#### `test_data_source_read_output_named_row`
Verifies data source read returning named rows.

#### `test_data_source_read_output_named_row_with_wrong_schema`
Verifies error handling when data source read returns named rows with schema mismatch.

#### `test_data_source_read_output_none`
Verifies error handling when data source read returns None.

#### `test_data_source_read_output_row`
Verifies data source read returning Row objects.

#### `test_data_source_read_output_tuple`
Verifies data source read returning tuples.

#### `test_data_source_read_output_with_partition`
Verifies data source read with custom partitioning.

#### `test_data_source_read_output_with_schema_mismatch`
Verifies error handling when data source read schema doesn't match expected schema.

#### `test_data_source_read_with_udf_perf_profiler`
udf profiler config should not enable data source profiling

#### `test_data_source_reader_pushdown_with_logging`
Verifies filter pushdown and logging for custom data sources.

#### `test_data_source_reader_with_logging`
Verifies logging during custom data source read operations.

#### `test_data_source_register`
Verifies registration and usage of custom data sources, including overwriting.

#### `test_data_source_segfault`
Verifies fault handling and traceback reporting when custom data source components segfault.

#### `test_data_source_type_mismatch`
Verifies error handling when data source returns wrong type for reader or writer.

#### `test_data_source_writer_with_logging`
Verifies logging and commit/abort behavior during custom data source write operations.

#### `test_extraneous_filter`
Verifies error handling when data source returns extraneous filters not requested by plan.

#### `test_filter_nested_column`
Verifies filter pushdown for nested columns in custom data sources.

#### `test_filter_pushdown`
Verifies filter pushdown behavior where some filters are applied and others are deferred to post-scan.

#### `test_filter_pushdown_disabled`
Verifies error handling when filter pushdown is required by plan but disabled in data source.

#### `test_filter_pushdown_error`
Verifies error handling when filter pushdown in data source throws an exception.

#### `test_filter_type`
Verifies pushdown of various filter types (EqualTo, IsNull, In, etc.) for custom data sources.

#### `test_filter_value_type`
Verifies pushdown of filters with various data types (int, float, string, etc.) for custom data sources.

#### `test_in_memory_data_source`
Verifies custom in-memory data source with specific partition behavior.

#### `test_read_with_invalid_return_row_type`
Verifies error handling when data source returns rows with invalid types.

#### `test_unsupported_filter`
Verifies that unsupported filters are not pushed down to custom data sources.

## File: `python/pyspark/sql/tests/connect/test_parity_python_streaming_datasource.py`

### Class: `PythonStreamingDataSourceParityTests`

#### `test_basic_data_source_stream_reader_class`
Verifies basic functionality of custom DataSourceStreamReader class.

#### `test_basic_streaming_data_source_class`
Verifies basic functionality of custom DataSource class for streaming.

#### `test_simple_stream_reader`
Verifies custom SimpleDataSourceStreamReader with basic streaming operations.

#### `test_simple_stream_reader_empty_iterator_start_equals_end_allowed`
When read() returns end == start with an empty iterator, no exception and no cache entry.

#### `test_simple_stream_reader_offset_did_not_advance_raises`
Validate that returning end == start with non-empty data raises SIMPLE_STREAM_READER_OFFSET_DID_NOT_ADVANCE.

#### `test_simple_stream_reader_trigger_available_now`
Verifies SimpleDataSourceStreamReader with trigger available now.

#### `test_stream_arrow_writer`
Test DataSourceStreamArrowWriter with Arrow RecordBatch format.

#### `test_stream_reader`
Verifies DataSourceStreamReader with standard streaming operations.

#### `test_stream_reader_admission_control_processing_time_trigger`
Verifies streaming with admission control and processing time trigger.

#### `test_stream_reader_admission_control_trigger_once`
Verifies streaming with admission control and trigger once.

#### `test_stream_reader_old_latest_offset_signature`
Verifies streaming with data source that uses old latestOffset signature.

#### `test_stream_reader_pyarrow`
Verifies streaming with data source returning PyArrow record batches.

#### `test_stream_reader_trigger_available_now`
Verifies streaming with data source and trigger available now.

#### `test_stream_writer`
Verifies custom DataSourceWriter for streaming operations, including commit and abort.

## File: `python/pyspark/sql/tests/connect/test_parity_readwriter.py`

### Class: `ReadwriterParityTests`

#### `test_binary_type`
Test that binary type in data sources respects binaryAsBytes config

#### `test_bucketed_write`
Verifies writing bucketed tables with various configurations.

#### `test_cached_table`
Verifies join operations on cached tables.

#### `test_changes_rejects_user_schema`
Verifies that changes API rejects user-provided schema.

#### `test_cluster_by`
Verifies writing clustered tables with various configurations.

#### `test_insert_into`
Verifies insertInto behavior with different modes and overwrite options.

#### `test_save`
Verifies that save errors out when path is not specified.

#### `test_save_and_load`
Verifies saving and loading DataFrames in various formats and modes.

#### `test_save_and_load_builder`
Verifies saving and loading DataFrames using builder pattern.

#### `test_streaming_changes_rejects_user_schema`
Verifies that streaming changes API rejects user-provided schema.

### Class: `ReadwriterV2ParityTests`

#### `test_api`
Verifies basic API of DataFrameWriterV2.

#### `test_cluster_by`
Verifies writing clustered tables with DataFrameWriterV2.

#### `test_create`
Verifies creating tables with DataFrameWriterV2.

#### `test_create_without_provider`
Verifies creating tables without specifying provider with DataFrameWriterV2.

#### `test_partitioning_functions`
Verifies partitioning functions in DataFrameWriterV2.

#### `test_table_overwrite`
Verifies table overwrite with DataFrameWriterV2 fails if table doesn't exist.

## File: `python/pyspark/sql/tests/connect/test_parity_repartition.py`

### Class: `DataFrameRepartitionParityTests`

#### `test_repartition_by_id`
Verifies repartitionById behavior.

#### `test_repartition_by_id_error_invalid_num_partitions`
Verifies error handling for invalid number of partitions in repartitionById.

#### `test_repartition_by_id_error_non_int_type`
Verifies error handling for non-integer partition column in repartitionById.

#### `test_repartition_by_id_negative_values`
Verifies repartitionById with negative partition expression values.

#### `test_repartition_by_id_null_values`
Verifies repartitionById with null partition expression values going to partition 0.

#### `test_repartition_by_id_out_of_range`
Verifies repartitionById behavior when partition ids are out of range.

#### `test_repartition_by_id_string_column_name`
Verifies repartitionById referencing partition column by string name.

## File: `python/pyspark/sql/tests/connect/test_parity_serde.py`

### Class: `SerdeParityTests`

#### `test_BinaryType_serialization`
Verifies serialization of BinaryType data.

#### `test_bytes_as_binary_type`
Verifies that Python bytes are treated as BinaryType.

#### `test_datetime_at_epoch`
Verifies serialization of datetime at epoch.

#### `test_filter_with_datetime`
Verifies filtering with datetime and date values.

#### `test_filter_with_datetime_timezone`
Verifies filtering with datetime values containing timezone info.

#### `test_int_array_serialization`
Verifies serialization of integer arrays.

#### `test_ntz_from_internal`
Verifies conversion of internal timestamp values to datetime objects without timezone.

#### `test_select_null_literal`
Verifies selecting null literal.

#### `test_serialize_nested_array_and_map`
Verifies serialization of nested arrays and maps.

#### `test_struct_in_map`
Verifies that Row objects can be used as keys or values in maps.

#### `test_time_with_timezone`
Verifies serialization of datetime with timezone info.

## File: `python/pyspark/sql/tests/connect/test_parity_sql.py`

### Class: `SQLParityTests`

#### `test_args_dict`
Verifies SQL queries with named arguments provided as a dictionary.

#### `test_args_list`
Verifies SQL queries with positional arguments provided as a list.

#### `test_kwargs_dataframe`
Verifies SQL queries with DataFrame arguments provided as kwargs.

#### `test_kwargs_dataframe_with_column`
Verifies SQL queries with DataFrame arguments referencing columns provided as kwargs.

#### `test_kwargs_literal`
Verifies SQL queries with literal arguments provided as kwargs.

#### `test_kwargs_literal_multiple_ref`
Verifies SQL queries referencing the same kwarg multiple times.

#### `test_nested_dataframe`
Verifies SQL queries with nested DataFrame arguments.

#### `test_nested_view`
Verifies SQL queries with nested views created from queries with arguments.

## File: `python/pyspark/sql/tests/connect/test_parity_stat.py`

### Class: `DataFrameStatParityTests`

#### `test_freqItems`
Verifies that the freqItems method (both on DataFrame and DataFrame.stat) correctly generates the expected protocol buffer plan with specified columns and support threshold.

#### `test_melt_groupby`
Verifies that chaining melt (unpivot) and groupby operations on a DataFrame works correctly and produces the expected number of rows in the result.

#### `test_replace`
Verifies that replace (and na.replace) operations generate correct protocol buffer plans with appropriate column subsets and value replacement mappings for both numeric and string values.

#### `test_unpivot`
Verifies that unpivot operations generate correct protocol buffer plans, handling both cases with explicit value columns and cases where value columns are inferred (None).

#### `test_unpivot_negative`
Verifies that unpivot raises appropriate AnalysisException errors for invalid usages, such as specifying no value columns or specifying value columns without a common data type.

## File: `python/pyspark/sql/tests/connect/test_parity_subquery.py`

### Class: `SubqueryParityTests`

#### `test_correlated_scalar_subquery`
Verifies correlated scalar subqueries in various contexts (WHERE clause, SELECT list, without explicit .outer(), null-safe equality, aggregate queries, non-equal conditions, and disjunctive conditions) by comparing results against equivalent SQL queries. It also checks that a subquery returning too many rows correctly raises a SCALAR_SUBQUERY_TOO_MANY_ROWS runtime exception.

#### `test_exists_subquery`
Verifies EXISTS and NOT EXISTS subqueries, including their usage within OR conditions, by comparing DataFrame operations against equivalent SQL queries.

#### `test_in_subquery`
Verifies IN and NOT IN subqueries, including usage with structs, within OR conditions, complex conditions, and same columns in subquery, comparing against equivalent SQL.

#### `test_lateral_join_in_between_regular_joins`
Verifies a complex query containing a lateral join placed between regular joins, comparing DataFrame results with the equivalent SQL query.

#### `test_lateral_join_inside_subquery`
Verifies usage of lateral joins inside a scalar subquery within a WHERE clause, comparing with equivalent SQL.

#### `test_lateral_join_reference_preceding_from_clause_items`
Verifies that a lateral join can correctly reference columns from preceding items in the FROM clause.

#### `test_lateral_join_with_aggregation_and_correlated_predicates`
Verifies lateral join involving aggregation and correlated predicates, comparing with equivalent SQL.

#### `test_lateral_join_with_correlated_predicates`
Verifies lateral joins with simple correlated equality and inequality predicates.

#### `test_lateral_join_with_different_join_types`
Verifies lateral joins with different join types (inner, left, cross) and ensures unsupported types (like right) raise an appropriate UNSUPPORTED_JOIN_TYPE AnalysisException.

#### `test_lateral_join_with_single_column_select`
Verifies lateral joins where the subquery simply selects a single column (correlated or not), comparing with equivalent SQL.

#### `test_lateral_join_with_star_expansion`
Verifies lateral joins involving star expansion (SELECT *) in the subquery.

#### `test_lateral_join_with_subquery_alias`
Verifies lateral joins where the subquery is aliased and its columns are renamed in the alias.

#### `test_lateral_join_with_table_valued_functions`
Verifies lateral joins involving Table Valued Functions (TVFs) like RANGE, EXPLODE, and EXPLODE_OUTER.

#### `test_lateral_join_with_table_valued_functions_and_join_conditions`
Verifies lateral joins with TVFs and explicit join conditions.

#### `test_multiple_lateral_joins`
Verifies chaining multiple lateral joins where later joins can reference columns produced by earlier ones.

#### `test_nested_lateral_joins`
Verifies nested lateral joins (a lateral join inside another lateral join).

#### `test_noop_outer`
Verifies that calling .outer() on a column resolved within the current scope is a no-op, and ensures that an unresolved column in .outer() raises an UNRESOLVED_COLUMN.WITH_SUGGESTION AnalysisException.

#### `test_scalar_subquery_against_local_relations`
Verifies scalar subqueries constructed against local DataFrames (not registered as temp views), comparing with equivalent SQL.

#### `test_scalar_subquery_inside_lateral_join`
Verifies scalar subquery placed inside the subquery of a lateral join.

#### `test_scalar_subquery_with_missing_outer_reference`
Verifies a scalar subquery where the outer reference doesn't use the explicit .outer() operator, but relies on implicit resolution.

#### `test_simple_uncorrelated_scalar_subquery`
Verifies simple uncorrelated scalar subqueries returning integers, strings, or 0 rows, comparing against SQL.

#### `test_subquery_in_drop`
Verifies using a scalar subquery to determine the column name to drop from a DataFrame.

#### `test_subquery_in_join_condition`
Verifies using a scalar subquery within a join condition.

#### `test_subquery_in_repartition`
Verifies using a scalar subquery to determine the number of partitions for repartition.

#### `test_subquery_in_transpose`
Verifies that using a scalar subquery as the index column in transpose raises an appropriate TRANSPOSE_INVALID_INDEX_COLUMN AnalysisException because it requires an atomic attribute.

#### `test_subquery_in_unpivot`
Verifies usage of subquery in unpivot by calling a helper method.

#### `test_subquery_in_with_columns`
Verifies adding columns containing values computed by scalar subqueries (using withColumn).

#### `test_subquery_in_with_columns_renamed`
Verifies renaming columns inside a subquery before extracting it as a scalar value.

#### `test_subquery_with_generator_and_tvf`
Verifies using a scalar subquery as an argument to generator functions like explode within SELECT and TVF contexts.

#### `test_uncorrelated_scalar_subquery_with_view`
Verifies uncorrelated scalar subqueries running against registered temp views, checking operations like limit, negative max, empty results, and nested subqueries.

## File: `python/pyspark/sql/tests/connect/test_parity_tvf.py`

### Class: `TVFParityTestsMixin`

#### `test_collations`
Verifies that the collations() TVF returns the same result as the SQL equivalent.

#### `test_explode`
Verifies explode, posexplode_outer, and explode_outer functions on DataFrames with both array and map fields, including handling of empty and null collections.

#### `test_explode_outer`
Verifies that the explode_outer TVF returns the same results as SQL for arrays, maps, empty collections, and nulls.

#### `test_explode_outer_with_lateral_join`
Verifies lateral joins with the explode_outer TVF against equivalent SQL.

#### `test_explode_with_lateral_join`
Verifies lateral joins with the explode TVF against equivalent SQL.

#### `test_inline`
Verifies inline and inline_outer functions for exploding an array of structs into rows, handling null structs and empty arrays.

#### `test_inline_outer`
Verifies that the inline_outer TVF works correctly with arrays of structs, empty arrays, and arrays with nulls, comparing against SQL.

#### `test_inline_outer_with_lateral_join`
Verifies lateral joins with the inline_outer TVF.

#### `test_inline_with_lateral_join`
Verifies lateral joins with the inline TVF.

#### `test_json_tuple`
Verifies the json_tuple TVF for extracting fields from a JSON string, and ensures that providing no fields raises a CANNOT_BE_EMPTY PySparkValueError.

#### `test_json_tuple_with_lateral_join`
Verifies lateral joins with the json_tuple TVF, handling null and invalid JSON inputs, and using it with a WHERE clause.

#### `test_posexplode`
Verifies the posexplode TVF for returning elements and their positions from arrays or maps, checking normal, empty, and null cases against SQL.

#### `test_posexplode_outer`
Verifies the posexplode_outer TVF for returning elements and positions, including outer join semantics for empty/null collections, compared with SQL.

#### `test_posexplode_outer_with_lateral_join`
Verifies lateral joins with the posexplode_outer TVF.

#### `test_posexplode_with_lateral_join`
Verifies lateral joins with the posexplode TVF.

#### `test_sql_keywords`
Verifies that the sql_keywords() TVF returns correct results matching SQL.

#### `test_stack`
Verifies the stack TVF for reshaping values into rows, comparing with SQL.

#### `test_stack_with_lateral_join`
Verifies lateral joins with the stack TVF in various join combinations (cross, left inner with ON condition), comparing against SQL.

#### `test_variant_explode`
Verifies the variant_explode TVF on JSON arrays, objects, empty collections, and non-array/object types, comparing results against SQL.

#### `test_variant_explode_outer`
Verifies the variant_explode_outer TVF on JSON arrays, objects, empty collections, and non-array/object types, verifying outer join behavior against SQL.

#### `test_variant_explode_outer_with_lateral_join`
Verifies lateral joins with the variant_explode_outer TVF.

#### `test_variant_explode_with_lateral_join`
Verifies lateral joins with the variant_explode TVF.

## File: `python/pyspark/sql/tests/connect/test_parity_types.py`

### Class: `TypesParityTests`

#### `test_access_nested_types`
Verifies that nested data types (array, struct, map) can be accessed using both bracket notation and explicit getItem/getField methods.

#### `test_apply_schema`
Verifies schema application (calls super).

#### `test_apply_schema_to_dict_and_rows`
Verifies applying schema to dicts and rows (calls super).

#### `test_apply_schema_to_row`
Verifies applying schema to Row objects (calls super).

#### `test_apply_schema_with_nullable_udt`
Verifies applying schema containing nullable User Defined Types (UDTs) to data with both present and null UDT values.

#### `test_apply_schema_with_udt`
Verifies applying schema containing non-nullable UDTs.

#### `test_array_type_from_json`
Verifies ArrayType.fromJson constructor, including handling of collations map parameter.

#### `test_array_types`
Verifies inference and creation of DataFrame from native python arrays of various types (unicode, float, double, signed/unsigned ints), and ensures that unsupported types raise expected CANNOT_INFER_TYPE_FOR_FIELD errors.

#### `test_cal_interval_in_collect`
Verifies calendar interval collection (calls super).

#### `test_calendar_interval_type`
Verifies that a SQL query returning an interval is resolved with CalendarIntervalType in the schema.

#### `test_calendar_interval_type_constructor`
Verifies constructor and simple string representation for CalendarIntervalType, ensuring it rejects positional arguments.

#### `test_calendar_interval_type_with_sf`
Verifies that using make_interval function results in CalendarIntervalType in schema.

#### `test_cast_to_string_with_udt`
Verifies that fields with UDT can be cast to string, invoking their respective string representations.

#### `test_cast_to_udt_with_udt`
Verifies that casting between two different UDTs is unsupported and throws AnalysisException.

#### `test_collated_string`
Verifies that collated strings are correctly handled during datatype-to-proto and proto-to-datatype conversions, ensuring the collation is preserved.

#### `test_complex_nested_udt_in_df`
Verifies that complex nested UDTs (like an array of UDTs resulting from collect_list) can be used in group-by aggregations and passed to both standard and Arrow-optimized UDFs.

#### `test_convert_list_to_str`
Verifies that creating a DataFrame with a list value for a string column correctly converts the list representation to a string.

#### `test_convert_row_to_dict`
Verifies that converting a Row containing nested arrays and maps of Rows to a dictionary using asDict() works correctly both for local Rows and Rows retrieved from a DataFrame created from them.

#### `test_create_dataframe_from_dataclasses`
Verifies that creating a DataFrame from a python dataclass instance correctly infers the schema and populates the data, matching asdict() representation.

#### `test_create_dataframe_from_dict_respects_schema`
Verifies that creating a DataFrame from a dictionary respects the provided schema column names, even if the dictionary keys are different.

#### `test_create_dataframe_from_objects`
Verifies inference of schema from custom Python objects (having named fields) when creating a DataFrame.

#### `test_create_dataframe_schema_mismatch`
Verifies schema mismatch behavior (calls super).

#### `test_daytime_interval_type`
Verifies inference and explicit schema creation for DayTimeIntervalType from datetime.timedelta objects, and checks resolution of interval literals in SQL queries.

#### `test_daytime_interval_type_constructor`
Verifies DayTimeIntervalType constructors with different start and end fields, and ensures that invalid casting arguments raise appropriate INVALID_INTERVAL_CASTING runtime errors.

#### `test_from_ddl`
Verifies DataType.fromDDL for parsing DDL strings representing various types including longs, structs, variants, and time with precision.

#### `test_geography_json_serde`
Verifies JSON serialization and deserialization for GeographyType, validating both valid DDL strings and asserting failure on invalid formats.

#### `test_geometry_json_serde`
Verifies JSON serialization and deserialization for GeometryType, validating both valid DDL strings and asserting failure on invalid formats.

#### `test_geospatial_create_dataframe`
Verifies creation of DataFrame with geospatial types (Geometry, Geography) using explicit schema, from lists of tuples, dicts, and Rows, comparing results. It also checks that schema mismatches raise appropriate GEO_ENCODER_SRID_MISMATCH_ERROR exceptions.

#### `test_geospatial_create_dataframe_rdd`
Verifies geospatial DataFrame creation from RDD (calls super).

#### `test_geospatial_encoding`
Verifies that generating Geometry and Geography types from WKB bytes using specific functions returns appropriate types with correct properties.

#### `test_geospatial_mixed_check_srid_validity`
Verifies that attempting to create a DataFrame with mixed SRIDs within the same column raises an ST_INVALID_SRID_VALUE IllegalArgumentException.

#### `test_geospatial_result_encoding`
Verifies that SQL queries returning Geometry and Geography types correctly produce them mapped back to custom python classes.

#### `test_geospatial_schema_inferrence`
Verifies schema inference when creating a DataFrame from instances of geospatial types, asserting resolution to appropriate specific or ANY SRID types based on provided values.

#### `test_hashable`
Verifies that instances of all core PySpark SQL data types can be hashed.

#### `test_infer_array_element_type_empty`
Verifies that DataFrame schema inference for arrays can correctly handle empty arrays and arrays with nulls, by inspecting all rows instead of only the first element.

#### `test_infer_array_element_type_empty_rdd`
Verifies inference of array element type from empty arrays with RDDs (calls super).

#### `test_infer_array_element_type_with_struct`
Verifies that schema inference for an array of dictionaries infers a single struct with all fields merged when appropriate configuration is enabled, and checks the legacy behavior when looking at only the first element.

#### `test_infer_array_merge_element_types`
Verifies that schema inference for arrays merges element types across all rows, and checks that legacy behavior (inferring only from first element) raises errors when appropriate. Also checks error conditions for arrays with only nulls, empty arrays, and conflicting types.

#### `test_infer_array_merge_element_types_with_rdd`
Verifies inference of array element types with RDDs (calls super).

#### `test_infer_binary_type`
Verifies inference of binary type (calls super).

#### `test_infer_long_type`
Verifies inference of long type (calls super).

#### `test_infer_map_merge_pair_types_with_rdd`
Verifies inference of map pair types with RDDs (calls super).

#### `test_infer_map_pair_type_empty`
Verifies that DataFrame schema inference for maps can correctly handle empty maps and maps with nulls by inspecting all rows.

#### `test_infer_map_pair_type_empty_rdd`
Verifies inference of map pair type from empty maps with RDDs (calls super).

#### `test_infer_map_pair_type_with_nested_maps`
Verifies inference of schema for nested maps.

#### `test_infer_nested_array_element_type_with_struct`
Verifies that nested arrays respect the legacy configuration for inferring element type from the first element.

#### `test_infer_nested_dict_as_struct`
Verifies that nested dictionaries are inferred as struct types when the appropriate configuration is enabled.

#### `test_infer_nested_dict_as_struct_with_rdd`
Verifies inference of nested dict as struct with RDDs (calls super).

#### `test_infer_nested_schema`
Verifies inference of nested schema (calls super).

#### `test_infer_schema`
Verifies inference of schema (calls super).

#### `test_infer_schema_not_enough_names`
Verifies that when not enough column names are provided during DataFrame creation, the remaining columns are given default names (e.g., _2).

#### `test_infer_schema_specification`
Verifies inference of schema for a wide variety of Python data types (boolean, long, string, date, timestamp, interval, double, array, map, struct, decimal, etc.) in a single DataFrame creation call. Also tests behavior with TIMESTAMP_NTZ configuration enabled and in different timezones.

#### `test_infer_schema_to_local`
Verifies inference of schema to local (calls super).

#### `test_infer_schema_upcast_boolean_to_string`
Verifies that boolean values are upcast to strings when mixed with string values in the same column during schema inference.

#### `test_infer_schema_upcast_float_to_string`
Verifies that float values are upcast to strings when mixed with string values in the same column during schema inference.

#### `test_infer_schema_upcast_int_to_string`
Verifies that int values are upcast to strings when mixed with string values in the same column (calls super).

#### `test_infer_schema_with_udt`
Verifies that schemas are correctly inferred for DataFrames created from Rows containing User Defined Types (UDTs), both for standard UDTs and Python-only UDTs.

#### `test_infer_schema_with_udt_with_column_names`
Verifies that schemas are correctly inferred for DataFrames created from tuples containing UDTs when column names are explicitly provided.

#### `test_infer_variant_type`
Verifies that creating a DataFrame from a Row containing a VariantVal correctly infers the column as VariantType and preserves the parsed JSON value and metadata.

#### `test_map_type_from_json`
Verifies MapType.fromJson constructor, including handling of optional collationsMap parameter to specify collations for keys and values.

#### `test_merge_type`
Verifies internal _merge_type function for merging various PySpark data types (Long, Null, Array, Map, Struct) and ensures that attempting to merge incompatible types raises appropriate CANNOT_MERGE_TYPE PySparkTypeErrors.

#### `test_metadata_null`
Verifies that creating a DataFrame with a schema where StructField metadata is None or contains null values works correctly.

#### `test_negative_decimal`
Verifies that casting to a decimal type with a negative scale works correctly when allowNegativeScaleOfDecimal legacy configuration is enabled.

#### `test_nested_udt_in_df`
Verifies that DataFrames can be created and collected with schemas containing arrays and maps of UDTs.

#### `test_parquet_with_udt`
Verifies that writing and reading DataFrames with UDT fields to/from Parquet format preserves the UDT data correctly.

#### `test_parse_datatype_json_string`
Verifies _parse_datatype_json_string for restoring instances of all core PySpark data types from their JSON representations.

#### `test_parse_datatype_string`
Verifies _parse_datatype_string for parsing shorthand string representations of various data types (int, string, char, varchar, decimal, array, map, struct, variant, time).

#### `test_rdd_with_udt`
Verifies UDTs with RDDs (calls super).

#### `test_repr`
Verifies that calling _repr_html_() on a plan generated by executing a SQL query or creating a range DataFrame returns HTML representations containing expected keyword markers.

#### `test_schema_with_bad_collations_provider`
Verifies that parsing a schema JSON string with an invalid/unsupported collation provider raises a PySparkValueError.

#### `test_schema_with_collations_json_ser_de`
Verifies schema with collations JSON serialization/deserialization (calls super).

#### `test_schema_with_collations_on_non_string_types`
Verifies that attempting to parse a schema JSON string with collations specified on non-string types (like integers, arrays of integers, or nested maps with integer values) raises appropriate PySparkTypeErrors.

#### `test_simple_udt_in_df`
Verifies that DataFrames can be created and collected with simple UDT fields.

#### `test_spark48834_from_ddl_matches_udf_schema_string`
Verifies that DataType.fromDDL produces schemas matching those produced by specifying the same DDL strings as return types in udf(), and that both raise identical parse exceptions for invalid inputs.

#### `test_string_type_simple_string`
Verifies that StringType returns appropriate simple strings, including cases where a specific collation is assigned.

#### `test_struct_type`
Verifies StructType behavior including creation via add, equality comparisons, length, item access by name/index/slice, and raising appropriate index/key errors.

#### `test_to_ddl`
Verifies that schemas can be correctly converted to their DDL string representations via the .toDDL() method for various combinations of types (including non-nullable fields).

#### `test_tree_string`
Verifies treeString method for generating tree-like representations of complex schemas (structs, arrays, maps) with various depth limits.

#### `test_tree_string_for_builtin_types`
Verifies treeString output for a schema containing all supported built-in types.

#### `test_udf_with_udt`
Verifies chaining operations involving UDFs and UDTs, testing various combinations of UDT inputs and outputs, arrays of UDTs, and basic type conversions.

#### `test_udt`
Verifies UDTs (calls super).

#### `test_udt_with_none`
Verifies that UDFs returning UDTs can correctly handle and return None values.

#### `test_union_with_udt`
Verifies that performing a union operation on two DataFrames with schemas containing UDTs preserves the UDT data correctly.

#### `test_variant_to_pandas`
Verifies that converting a DataFrame with a VariantType column to a pandas DataFrame correctly maps the values to objects that can be converted back to JSON.

#### `test_variant_type`
Verifies VariantType behavior including creation, parsing from JSON strings, conversion back to JSON strings, conversion to Python types, and proper handling within DataFrame creation (createDataFrame) with both values and nulls.

#### `test_yearmonth_interval_type`
Verifies that SQL queries returning year-month intervals are resolved with correct YearMonthIntervalType in schema, supporting both year-to-month and individual year or month forms.

#### `test_yearmonth_interval_type_constructor`
Verifies constructor and simple string representation for YearMonthIntervalType with various start and end fields, and ensures invalid configurations raise appropriate INVALID_INTERVAL_CASTING errors.

#### `test_ym_interval_in_collect`
Verifies year-month interval collection (calls super).

## File: `python/pyspark/sql/tests/connect/test_parity_udf.py`

### Class: `UDFParityTests`

#### `test_broadcast_in_udf`
Verifies broadcast variables inside UDFs (calls super).

#### `test_chained_udf`
Verifies chaining UDF calls within SQL queries, including nested calls and combinations with arithmetic operations.

#### `test_chained_udfs_with_variant`
Verifies chaining UDFs where the first UDF returns a VariantVal and the second UDF operates on that variant (converting it to string), covering direct variants, struct containing variants, array of variants, and map containing variants.

#### `test_complex_return_types`
Verifies that UDFs can correctly return complex types like arrays, maps, and structs by returning the same values as input.

#### `test_datasource_with_udf`
Verifies that UDFs can be used in projections and filters on DataFrames loaded from various data sources (CSV, SimpleScanSource, SimpleDataSourceV2).

#### `test_err_udf_init`
Verifies appropriate error handling during UDF initialization (calls super).

#### `test_err_udf_registration`
Verifies that attempting to register a non-callable object as a UDF raises a PySparkTypeError with error class NOT_EXPECTED_TYPE.

#### `test_file_dsv2_with_udf_filter`
Verifies that filtering with a UDF on a DataFrame read from Parquet using DSv2 works correctly and returns an empty result when the UDF always returns false.

#### `test_kwargs`
Verifies that UDFs can accept keyword arguments (kwargs) in both DataFrame operations and SQL queries, and that invalid use cases (duplicate assignment or unexpected positional arguments) raise proper AnalysisException errors.

#### `test_multiple_udfs`
Verifies registration and usage of multiple distinct UDFs within a single SQL query, including chained calls to the same or different UDFs.

#### `test_multiple_udfs_with_logging`
Verifies that multiple UDFs executing in parallel correctly log warning messages to the Python worker log table when logging is enabled.

#### `test_named_arguments`
Verifies that UDFs can be called with named arguments in both DataFrame API and SQL queries.

#### `test_named_arguments_and_defaults`
Verifies that UDFs support both named arguments and default parameter values in both DataFrame API and SQL queries.

#### `test_named_arguments_negative`
Verifies that invalid uses of named arguments in UDFs (like duplicate assignments, unexpected keywords, or mixing positional and named arguments incorrectly) raise appropriate AnalysisException or PythonException errors.

#### `test_nested_array`
Verifies that UDFs can correctly handle and return nested arrays (arrays of arrays), asserting string representation on input and modifying elements on output.

#### `test_nested_map`
Verifies that UDFs can correctly handle and return nested maps (maps of maps), asserting string representation on input and modifying elements on output.

#### `test_nested_struct`
Verifies that UDFs can correctly handle and return nested structs (structs containing other structs), asserting string representation on input and returning the object as output.

#### `test_non_existed_udaf`
Verifies that attempting to register a non-existent Java UDAF raises an AnalysisException.

#### `test_non_existed_udf`
Verifies that attempting to register a non-existent Java UDF raises an AnalysisException.

#### `test_non_existed_udf_with_sql_context`
Verifies non-existent UDF with SQL context (calls super).

#### `test_nondeterministic_udf`
Verifies that non-deterministic UDFs (like those using random) are evaluated only once in chained UDF evaluations.

#### `test_nondeterministic_udf2`
Verifies that registering a non-deterministic UDF preserves its non-deterministic property, and that it behaves as expected when executed via SQL or DataFrame API. Also checks that pydoc.render_doc does not fail on these objects.

#### `test_nondeterministic_udf3`
Verifies non-deterministic UDF behavior (calls super).

#### `test_nondeterministic_udf_in_aggregate`
Verifies non-deterministic UDF in aggregate operations (calls helper).

#### `test_nonparam_udf_with_aggregate`
Verifies that calling a UDF without parameters on a DataFrame after a distinct operation works correctly.

#### `test_num_arguments`
Verifies UDF behavior with different number of arguments (0 and 1), ensuring correct results regardless of order in selection.

#### `test_python_udf_segfault`
Verifies that enabling faulthandler for Python UDFs correctly intercepts segmentation faults and raises an exception instead of crashing the worker.

#### `test_raise_stop_iteration`
Verifies that raising StopIteration inside a UDF is correctly handled and propagated as a PythonException.

#### `test_register_java_function`
Verifies registration of Java UDFs and execution via SQL queries, checking resolution with explicit return types and shorthand type strings.

#### `test_register_java_udaf`
Verifies registration of Java UDAFs and execution via SQL queries involving group-by aggregations.

#### `test_same_accumulator_in_udfs`
Verifies same accumulator in multiple UDFs (calls super).

#### `test_single_udf_with_repeated_argument`
Verifies that calling a registered UDF with repeated arguments (e.g., add_int(1, 1)) works correctly.

#### `test_timeout_util_with_udf`
Verifies that a UDF that sleeps longer than the specified timeout raises an exception.

#### `test_udf`
Verifies basic UDF functionality as normal function and decorator, with return types specified as DDL string and DataType object.

#### `test_udf2`
Verifies registering UDF and using it in SQL query with filter condition.

#### `test_udf3`
Verifies registering a UDF with two arguments and using it in a SQL query.

#### `test_udf_and_common_filter_in_join_condition`
Verifies complex join scenario with both a UDF condition and a standard equality filter condition.

#### `test_udf_as_join_condition`
Verifies using UDFs as join conditions (e.g., f("a") == f("b")) combined with other column equality conditions.

#### `test_udf_binary_type`
Verifies UDF returning binary type (calls super).

#### `test_udf_binary_type_in_nested_structures`
Verifies UDF returning binary type in nested structures (calls super).

#### `test_udf_cache`
Verifies that a DataFrame with a projected UDF can be cached and subsequently uses the in-memory scan in execution plan.

#### `test_udf_daytime_interval`
Verifies that UDFs can accept and return DayTimeIntervalType mapped to Python's datetime.timedelta.

#### `test_udf_defers_judf_initialization`
Verifies that UDF defers JUDF initialization (calls super).

#### `test_udf_empty_frame`
Verifies that calling a UDF on an empty DataFrame returns an empty result without errors.

#### `test_udf_globals_not_overwritten`
Verifies that UDF execution does not overwrite Python built-in globals (like map).

#### `test_udf_in_filter_on_top_of_join`
Verifies that using a UDF in a filter on top of a cross join works correctly.

#### `test_udf_in_filter_on_top_of_outer_join`
Verifies that using a UDF in a filter on top of a left outer join works correctly, even when the UDF is applied to a column generated by a previous withColumn.

#### `test_udf_in_generate`
Verifies using UDFs inside explode function, asserting correct behavior with list outputs and proper counting/aggregation of generated rows.

#### `test_udf_in_join_condition`
Verifies using a UDF as the ONLY join condition, asserting that it triggers the implicit cartesian product check (raising an exception) when cross joins are disabled, and succeeds when enabled.

#### `test_udf_in_left_outer_join_condition`
Verifies using a UDF in a left outer join condition where it refers to attributes from only one side, while the full condition refers to both sides.

#### `test_udf_in_subquery`
Verifies using a UDF inside a subquery that is part of a WHERE IN clause.

#### `test_udf_input_serialization_valuecompare_disabled`
Verifies that UDFs can correctly process inputs with struct types containing duplicate field values when certain optimizations are disabled.

#### `test_udf_kill_on_timeout`
Verifies that worker processes running a long UDF are killed when idle timeout triggers if appropriate configuration is enabled, raising a specific process termination exception.

#### `test_udf_not_supported_in_join_condition`
Verifies that attempting to use a Python UDF in the ON clause of non-inner joins (full, left, right, leftanti, leftsemi) raises an AnalysisException stating lack of support.

#### `test_udf_on_sql_context`
Verifies UDF on SQL context (calls super).

#### `test_udf_registration_return_type_none`
Verifies that when registering a UDF with returnType=None, it falls back to the default StringType.

#### `test_udf_registration_return_type_not_none`
Verifies UDF registration with return type not None (calls helper).

#### `test_udf_registration_returns_udf`
Verifies that registering a UDF returns a handle that can be used directly in subsequent DataFrame operations, comparing against equivalent SQL expressions.

#### `test_udf_registration_returns_udf_on_sql_context`
Verifies UDF registration returns UDF on SQL context (calls super).

#### `test_udf_should_not_accept_noncallable_object`
Verifies that attempting to construct a UserDefinedFunction with a non-callable object raises a TypeError.

#### `test_udf_timestamp_ntz`
Verifies that UDFs can accept and return TimestampNTZType mapped to Python's datetime.datetime without timezone handling.

#### `test_udf_with_256_args`
Verifies that UDFs can accept and execute properly with 256 arguments.

#### `test_udf_with_aggregate_function`
Verifies using UDFs in distinct and filter operations on columns, and inside group-by aggregations (agg) combined with built-in sum, and finally in select with UDF adding results.

#### `test_udf_with_array_type`
Verifies registering and using UDFs with array return types, and accessing map length in SQL queries.

#### `test_udf_with_callable`
Verifies that UDFs can be constructed with custom Python classes that implement the __call__ method.

#### `test_udf_with_char_varchar_return_type`
Verifies that attempting to use char/varchar or complex types containing them as UDF return types raises appropriate exceptions.

#### `test_udf_with_collated_string_types`
Verifies that UDFs can accept and return collated string types, preserving the collation in the result schema.

#### `test_udf_with_column_vector`
Verifies that UDFs work correctly when reading from Parquet with off-heap column vectors enabled or disabled.

#### `test_udf_with_complex_variant_input`
Verifies that UDFs can accept complex types containing variants (struct, array, map) as input and process them correctly.

#### `test_udf_with_complex_variant_output`
Verifies that UDFs can return complex types containing variants (struct, array, map) and that the results are correctly cast to string representations.

#### `test_udf_with_decorator`
Verifies basic UDF functionality with decorators, specifying return types in various ways (type objects, strings) and checking results against expected values and schemas.

#### `test_udf_with_filter_function`
Verifies using UDFs returning boolean in combination with other filter conditions.

#### `test_udf_with_input_file_name`
Verifies that input_file_name() built-in function returns correct file names when used as argument to a UDF.

#### `test_udf_with_input_file_name_for_hadooprdd`
Verifies UDF with input file name for HadoopRDD (calls super).

#### `test_udf_with_logging`
Verifies that UDFs can write to standard output and error, log messages with custom context, and record exceptions in a specific TVF when logging is enabled.

#### `test_udf_with_order_by_and_limit`
Verifies that applying a UDF to a column after sorting and limiting the DataFrame produces correct results.

#### `test_udf_with_partial_function`
Verifies that UDFs can be constructed with functools.partial functions having bound arguments.

#### `test_udf_with_pyspark_logger`
Verifies that UDFs using PySparkLogger can log warning messages with custom context to worker logs when enabled.

#### `test_udf_with_rand`
Verifies using UDF with rand() function.

#### `test_udf_with_string_return_type`
Verifies that UDFs can be registered with return types specified as strings (e.g., "integer", "struct<...>", "array<...>") and produce correct results.

#### `test_udf_with_udt`
Verifies that UDFs can accept and return User Defined Types (UDTs), testing various combinations of chaining UDT inputs and outputs with standard types and arrays of UDTs.

#### `test_udf_with_variant_input`
Verifies that UDFs can accept VariantType inputs and that they can be converted to strings correctly inside the UDF.

#### `test_udf_with_variant_output`
Verifies that UDFs can return VariantType outputs by constructing VariantVal objects in Python.

#### `test_udf_without_arguments`
Verifies that UDFs without arguments can be registered and called in SQL queries.

#### `test_udf_wrapper`
Verifies that the udf function correctly wraps callable objects, preserving docstrings, function handles, and return types for regular functions, classes with __call__, and functools.partial objects.

#### `test_worker_original_stdin_closed`
Verifies worker original stdin closed (calls super).

## File: `python/pyspark/sql/tests/connect/test_parity_udf_combinations.py`

### Class: `UDFCombinationsParityTests`

#### `test_combination_2`
Verifies all combinations of 2 scalar functions applied sequentially produce correct results.

#### `test_combination_3`
Verifies all combinations of 3 scalar functions applied sequentially produce correct results.

#### `test_combination_4`
Verifies all combinations of 4 scalar functions applied sequentially produce correct results.

#### `test_combination_5`
Verifies all combinations of 5 scalar functions applied sequentially produce correct results.

#### `test_combination_6`
Verifies all combinations of 6 scalar functions applied sequentially produce correct results.

## File: `python/pyspark/sql/tests/connect/test_parity_udf_profiler.py`

### Class: `UDFProfilerParityTests`

#### `test_perf_profiler_aggregate_in_pandas`
Verifies that the "perf" profiler works correctly for Pandas UDFs used in group-by aggregations.

#### `test_perf_profiler_arrow_udf`
Verifies that the "perf" profiler works correctly for Arrow UDFs, collecting profile results for distinct calls.

#### `test_perf_profiler_arrow_udf_agg`
Verifies that the "perf" profiler works correctly for Arrow UDFs used in aggregations.

#### `test_perf_profiler_arrow_udf_grouped_agg_iter`
Verifies that the "perf" profiler works correctly for Arrow UDFs processing iterators of arrays in grouped aggregations.

#### `test_perf_profiler_arrow_udf_iterator`
Verifies that the "perf" profiler works correctly for Arrow UDFs processing iterators of arrays, yielding iterators of arrays.

#### `test_perf_profiler_arrow_udf_window`
Verifies that the "perf" profiler works correctly for Arrow UDFs used in window operations.

#### `test_perf_profiler_clear`
Verifies that profile results can be cleared for specific UDFs or for all UDFs of a specific type or all types.

#### `test_perf_profiler_cogroup_apply_in_arrow`
Verifies that the "perf" profiler works correctly for applyInArrow on co-grouped DataFrames.

#### `test_perf_profiler_cogroup_apply_in_pandas`
Verifies that the "perf" profiler works correctly for applyInPandas on co-grouped DataFrames.

#### `test_perf_profiler_group_apply_in_arrow`
Verifies that the "perf" profiler works correctly for applyInArrow on grouped DataFrames.

#### `test_perf_profiler_group_apply_in_pandas`
Verifies that the "perf" profiler works correctly for applyInPandas on grouped DataFrames.

#### `test_perf_profiler_map_in_arrow`
Verifies that the "perf" profiler works correctly for mapInArrow operations.

#### `test_perf_profiler_map_in_pandas`
Verifies that the "perf" profiler works correctly for mapInPandas operations.

#### `test_perf_profiler_pandas_udf`
Verifies that the "perf" profiler works correctly for Pandas UDFs, collecting profile results for distinct calls.

#### `test_perf_profiler_pandas_udf_grouped_agg_iter`
Verifies that the "perf" profiler works correctly for Pandas UDFs processing iterators of series in grouped aggregations.

#### `test_perf_profiler_pandas_udf_iterator`
Verifies that the "perf" profiler works correctly for Pandas UDFs processing iterators of series, yielding iterators of series.

#### `test_perf_profiler_pandas_udf_window`
Verifies that the "perf" profiler works correctly for Pandas UDFs used in window operations.

#### `test_perf_profiler_render`
Verifies that profile results can be rendered as SVG (using flameprof if available) and that invalid render types raise appropriate exceptions. Also checks custom renderers.

#### `test_perf_profiler_udf`
Verifies basic functionality of the "perf" profiler for UDFs, including enabling/disabling via configuration, showing profile results, and dumping them to a directory.

#### `test_perf_profiler_udf_multiple_actions`
Verifies that separate profile results are collected for separate actions when plan cache is disabled.

#### `test_perf_profiler_udf_registered`
Verifies that the "perf" profiler works correctly for registered UDFs called via SQL.

#### `test_perf_profiler_udf_with_arrow`
Verifies basic functionality of the "perf" profiler for Arrow-optimized Python UDFs.

### Class: `UDFProfilerWithoutPlanCacheParityTests`

#### `test_perf_profiler_aggregate_in_pandas`
Verifies that the "perf" profiler works correctly for Pandas UDFs used in group-by aggregations when plan cache is disabled.

#### `test_perf_profiler_arrow_udf`
Verifies that the "perf" profiler works correctly for Arrow UDFs, collecting profile results for distinct calls when plan cache is disabled.

#### `test_perf_profiler_arrow_udf_agg`
Verifies that the "perf" profiler works correctly for Arrow UDFs used in aggregations when plan cache is disabled.

#### `test_perf_profiler_arrow_udf_grouped_agg_iter`
Verifies that the "perf" profiler works correctly for Arrow UDFs processing iterators of arrays in grouped aggregations when plan cache is disabled.

#### `test_perf_profiler_arrow_udf_iterator`
Verifies that the "perf" profiler works correctly for Arrow UDFs processing iterators of arrays, yielding iterators of arrays when plan cache is disabled.

#### `test_perf_profiler_arrow_udf_window`
Verifies that the "perf" profiler works correctly for Arrow UDFs used in window operations when plan cache is disabled.

#### `test_perf_profiler_clear`
Verifies that profile results can be cleared for specific UDFs or for all UDFs of a specific type or all types when plan cache is disabled.

#### `test_perf_profiler_cogroup_apply_in_arrow`
Verifies that the "perf" profiler works correctly for applyInArrow on co-grouped DataFrames when plan cache is disabled.

#### `test_perf_profiler_cogroup_apply_in_pandas`
Verifies that the "perf" profiler works correctly for applyInPandas on co-grouped DataFrames when plan cache is disabled.

#### `test_perf_profiler_group_apply_in_arrow`
Verifies that the "perf" profiler works correctly for applyInArrow on grouped DataFrames when plan cache is disabled.

#### `test_perf_profiler_group_apply_in_pandas`
Verifies that the "perf" profiler works correctly for applyInPandas on grouped DataFrames when plan cache is disabled.

#### `test_perf_profiler_map_in_arrow`
Verifies that the "perf" profiler works correctly for mapInArrow operations when plan cache is disabled.

#### `test_perf_profiler_map_in_pandas`
Verifies that the "perf" profiler works correctly for mapInPandas operations when plan cache is disabled.

#### `test_perf_profiler_pandas_udf`
Verifies that the "perf" profiler works correctly for Pandas UDFs, collecting profile results for distinct calls when plan cache is disabled.

#### `test_perf_profiler_pandas_udf_grouped_agg_iter`
Verifies that the "perf" profiler works correctly for Pandas UDFs processing iterators of series in grouped aggregations when plan cache is disabled.

#### `test_perf_profiler_pandas_udf_iterator`
Verifies that the "perf" profiler works correctly for Pandas UDFs processing iterators of series, yielding iterators of series when plan cache is disabled.

#### `test_perf_profiler_pandas_udf_window`
Verifies that the "perf" profiler works correctly for Pandas UDFs used in window operations when plan cache is disabled.

#### `test_perf_profiler_render`
Verifies that profile results can be rendered as SVG (using flameprof if available) and that invalid render types raise appropriate exceptions when plan cache is disabled.

#### `test_perf_profiler_udf`
Verifies basic functionality of the "perf" profiler for UDFs when plan cache is disabled.

#### `test_perf_profiler_udf_multiple_actions`
Verifies that separate profile results are collected for separate actions when plan cache is disabled.

#### `test_perf_profiler_udf_registered`
Verifies that the "perf" profiler works correctly for registered UDFs called via SQL when plan cache is disabled.

#### `test_perf_profiler_udf_with_arrow`
Verifies basic functionality of the "perf" profiler for Arrow-optimized Python UDFs when plan cache is disabled.

## File: `python/pyspark/sql/tests/connect/test_parity_udtf.py`

### Class: `ArrowUDTFParityTests`

#### `test_array_output_type_casting`
This test evaluates automatic casting of a Python array/list yielded by a UDTF into various target return schemas specified in `udtf(...)`. It defines a UDTF that simply yields `([0, 1.1, 2],)`. It then iterates through twenty different target schemas (primitive types, strings, arrays of primitives, map, struct). For each, it expects either a specific resulting Row sequence where failed casts become `None` or string representations, or an exception (e.g. `AttributeError` for date/timestamp). It serves to ensure robust error handling and conversion behaviors for array outputs.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_decimal_round`
This test appears to assert a specific rounding behavior when storing a Python `Decimal` into a DataFrame. Note that the requested outcome indicates highly specific, slightly imprecise floating point approximation behavior on this platform which typically suggests checking for float-to-decimal conversion precision issues.

#### `test_df_asTable`
This test verifies the `DataFrame.asTable()` API when used as an input to a UDTF. It expects programmatic usage `func(df.asTable())` to match results with a SQL query using the `TABLE(SELECT id FROM range(0, 8))` operator on the registered function.

#### `test_df_asTable_chaining_methods`
This test deeply checks fluent builder-style method chaining on the `df.asTable()` object. It confirms you can chain `partitionBy(...)` and `orderBy(...)`, or `withSinglePartition()` in various combinations and with columns/expressions, and it accurately maps to the execution plan with row order matching expectations. The test also exercises error states, ensuring that illegal combinations like invoking `partitionBy()` twice, or `orderBy()` before any partition instructions raise `IllegalArgumentException`.

#### `test_docstring`
This test validates that standard docstrings placed in a UDTF's class definition and its lifecycle methods (`__init__`, `analyze`, `eval`, `terminate`) are correctly preserved and accessible via PySpark on the resulting UDTF wrapper class.

#### `test_eval_type`
This test confirms that requesting an Arrow-based execution vs non-Arrow execution on UDTF creation actually updates the `evalType` attribute internally to the correct integer representing either `SQL_TABLE_UDF` or `SQL_ARROW_TABLE_UDF`.

#### `test_eval_with_exception`
This test validates that uncaught runtime exceptions raised from within a UDTF's `eval` method are properly caught and re-thrown by the Spark runner as a `PythonException` identifying execution error in 'eval'.

#### `test_inconsistent_output_types`
This test observes what happens when a UDTF emits rows that are structurally inconsistent (an integer row followed by a list row). It checks both an integer target return schema and an array return schema. In both cases, the non-fitting value simply becomes a null cell in the dataset without aborting the query.

#### `test_init_with_exception`
This test handles error tracking when the instance constructor `__init__` of the UDTF class raises a runtime exception. It expects an execution error specifically pointing at the constructor.

#### `test_map_output_type_casting`
This is a complex casting test equivalent to Test 24 but starting from a raw Python dictionary yielded by the UDTF: `({"a": 0, "b": 1.1, "c": 2},)`. It checks 20 separate result schemas (including primitives, maps with different value types, array, structs) assessing whether dictionary keys maps to struct fields, or full string dumps happen for non-compatible targets like single primitive strings.

#### `test_nondeterministic_udtf`
This test evaluates a UDTF whose output is non-deterministic because it invokes Python's `random.random()`. The test makes sure the value yielded can still be queried successfully via SQL and DataFrame APIs. Note that since the random value is added with `int(...)` of `0<=r<1` (which is always `0`), the math still predictably produces `a + 0 = 1`.

#### `test_numeric_output_type_casting`
Similar to Test 24 and 33, this test observes casting behavior when the UDTF yields a simple integer value `(1,)` but requests other schemas. It shows that casting to string yields `"1"`, but invalid casts to binary or arrays simply result in `None` values without errors, and Struct requires strict compliance producing a labeled exception rather than a null row.

#### `test_numeric_string_output_type_casting`
This test executes similar schema mismatch experiments as Test 35, except the starting yielded value is the string `("1",)`. It demonstrates that numeric string casts to integers fail and yield `None` under arrow execution, while string-to-binary properly outputs mapped byte arrays.

#### `test_simple_udtf`
This is another simple UDTF test that doesn't use the SQL registry but invokes the resulting programmatic wrapper directly. It takes no inputs and returns a single row of two strings, verifying DataFrame construction directly from evaluated execution.

#### `test_simple_udtf_with_analyze`
This test verifies a UDTF without inputs that specifies its own return schema dynamically via the static `analyze` method. Both the programmatic wrapper invocation and regular execution through the SQL environment are expected to execute successfully and deduce column names and types properly without them being stated inside the outer `@udtf` decorator.

#### `test_string_output_type_casting`
This test performs automatic type casting operations starting with a non-numeric string value `("hello",)`. It expects successful conversion to string and binary, but attempts to convert this to numeric types or structured objects will safely yield `None` in execution rows.

#### `test_struct_output_type_casting_dict`
This test expands on Test 33, yielding a dictionary `({"a": 0, "b": 1.1, "c": 2},)` and targeting struct schemas with varying field types. It validates how well PySpark maps dictionary keys to requested struct fields, accurately dropping types or filling `None` when fields are mistyped in the Python source vs SQL schema destination.

#### `test_struct_output_type_casting_row`
This test calls a helper method `check_struct_output_type_casting_row` passing `PickleException`. It likely tests the behavior when yielding a Spark `Row` object from a UDTF intended to be cast to a struct, and specifically checks for behavior that leads to a `PickleException` (or expects it to be handled).

#### `test_terminate_with_exceptions`
This test ensures that if a UDTF's `terminate` method raises an exception (e.g., a `ValueError` during final aggregation), Spark safely traps this and raises a corresponding `PythonException` identifying execution breakdown in 'terminate'.

#### `test_udtf_access_spark_session`
This test calls the superclass implementation of `test_udtf_access_spark_session`. It likely tests whether a UDTF can access the active Spark session within its execution.

#### `test_udtf_access_spark_session_connect`
This test explicitly asserts that a UDTF running in Spark Connect cannot directly reference and execute a collection on an external DataFrame from outside scope during `eval` processing, as doing so requires access to an active session context that isn't provided on workers directly. It expects a failure indicating "NO_ACTIVE_SESSION".

#### `test_udtf_analyze_with_logging`
This test checks that Python standard logging works within the static `analyze` method of a UDTF. It enables worker logging via SQL configuration, runs a query containing a lateral join to the UDTF, and pulls the worker logs from a special table-valued function `spark.tvf.python_worker_logs()`. It asserts that the log message printed in `analyze` is correctly captured with precise level, message, and context identifiers.

#### `test_udtf_analyze_with_pyspark_logger`
This is a direct parallel to Test 45, testing Spark's own `PySparkLogger` instead of standard Python logging. It ensures that custom log properties added to the log message (such as `dt=x.dataType.json()`) are correctly captured and searchable as independent column attributes on the queried worker log data stream.

#### `test_udtf_arrow_sql_conf`
This test validates that changing the boolean SQL configuration `spark.sql.execution.pythonUDTF.arrow.enabled` toggles the resulting UDTF execution evaluation types back and forth between Arrow UDF and standard SQL table UDF values at runtime on definition.

#### `test_udtf_binary_type`
This test checks how binary parameters are handled by UDTFs under different configurations of the `spark.sql.execution.pyspark.binaryAsBytes` flag. Depending on whether it's enabled or not, the Python side might receive either primitive `bytes` or mutable `bytearray` representations.

#### `test_udtf_cleanup_with_exception_in_eval`
This test guarantees that a UDTF's `cleanup()` lifecycle method will still run correctly if a failure occurs in the mid-flight execution of the `eval` method. It observes file system traces written by the test UDTF, ensuring only "cleanup" is recorded without executing final "terminate" yields because the data pipeline failed midway.

#### `test_udtf_cleanup_with_exception_in_terminate`
This test verifies that the `cleanup` method of a User-Defined Table Function (UDTF) is executed even if the `terminate` method raises an exception. It creates a UDTF that writes "cleanup" to a temporary file in its `cleanup` method and raises an exception in its `terminate` method. The test asserts that the exception from `terminate` is raised and that the file contains "cleanup", proving that `cleanup` was called despite the error in `terminate`.

#### `test_udtf_decorator`
This test checks the basic functionality of the `@udtf` decorator. It defines a UDTF class with an `eval` method that yields the input and the input plus one. It verifies that calling this UDTF with a literal value returns the expected DataFrame containing a single row with the evaluated values.

#### `test_udtf_determinism`
This test checks the determinism properties of a UDTF. It verifies that a UDTF is marked as non-deterministic by default and that it can be explicitly marked as deterministic using the `asDeterministic` method.

#### `test_udtf_eval_returning_non_tuple`
This test verifies that a UDTF must return or yield tuples in its `eval` method. It checks several failure cases: yielding a single value instead of a tuple, returning a tuple (which is invalid because `eval` should yield or return an iterable of tuples), returning a list, and yielding a User-Defined Type (UDT) object directly. All these cases are expected to raise a `PythonException` with the error class `UDTF_INVALID_OUTPUT_ROW_TYPE`.

#### `test_udtf_eval_returning_tuple_with_struct_type`
This test verifies how UDTFs handle returning tuples when the schema specifies a struct type. In the successful case, the UDTF yields a tuple containing another tuple, which correctly maps to the specified struct type `struct<b: int, c: int>`. In the failure case, the UDTF yields flat values, which causes a schema mismatch error (`UDTF_RETURN_SCHEMA_MISMATCH`) because the engine expects a struct.

#### `test_udtf_eval_returning_udt`
This test verifies that a UDTF can successfully return a User-Defined Type (UDT) instance wrapped in a tuple. It defines a UDTF that takes two floats, multiplies them by 10, creates an `ExamplePoint` UDT instance, yields it within a tuple, and asserts that the resulting DataFrame matches the expected value.

#### `test_udtf_eval_taking_udt`
This test verifies that a UDTF can accept a User-Defined Type (UDT) instance as an argument. It creates a DataFrame with an `ExamplePoint` UDT, applies a UDTF that extracts and scales the point's coordinates using a `lateralJoin`, and verifies the output rows.

#### `test_udtf_eval_with_no_return`
This test checks the behavior of a UDTF when its `eval` method does not explicitly yield or return any rows. It defines two UDTFs: one with an empty body (`...`) and one that explicitly calls `return` without yielding. It asserts that both produce an empty DataFrame.

#### `test_udtf_eval_with_return_stmt`
This test confirms that a UDTF can return an iterable (like a list of tuples) from its `eval` method as an alternative to yielding. It registers a UDTF that returns two rows based on its two input arguments and checks that the output DataFrame contains both expected rows.

#### `test_udtf_init`
This test verifies that the `__init__` constructor method of a UDTF class is called correctly and can be used to store instance state (e.g., `self.key`). The UDTF uses this state in its `eval` method to populate one of the output columns, and the test checks the final DataFrame for the stored string.

#### `test_udtf_init_with_additional_args`
This test confirms that the `__init__` method of a UDTF cannot accept additional arguments (besides the implicit `self`). It defines a UDTF class with a constructor accepting an argument, and verifies that executing it raises a `PythonException` stating that the constructor has too many arguments.

#### `test_udtf_kill_on_timeout`
This test checks that a UDTF can be forcefully terminated if it exceeds a specified idle timeout. It uses Spark configurations to set an idle timeout of 1 second and enables killing on timeout. It tests two scenarios: a slow `eval` method that sleeps for 2 seconds, and a slow `analyze` method that sleeps for 2 seconds. Both scenarios are expected to fail with an exception indicating that the Python worker was terminated due to an idle timeout.

#### `test_udtf_no_eval`
This test verifies that defining a UDTF without an `eval` method results in a `PySparkAttributeError`. It attempts to create a UDTF with a method named `run` instead of `eval`, and asserts that the appropriate error class (`INVALID_UDTF_NO_EVAL`) and message are generated.

#### `test_udtf_nullable_check`
This test thoroughly validates the nullability constraints of various complex data types returned by UDTFs. It defines UDTFs yielding different values against schemas with specific nullability flags (e.g., non-nullable array elements or map values). The test asserts that a `PySparkRuntimeError` is raised whenever the UDTF yields a null in a field that was declared as non-nullable in the schema, while allowing nulls where they are allowed.

#### `test_udtf_pickle_error`
This test verifies that a descriptive `PySparkPicklingError` is raised when a UDTF cannot be serialized (pickled) due to containing unpicklable objects like a file handle in its closure. It defines a UDTF that references a local open file object, attempts to execute it, and checks for the specific error class `UDTF_SERIALIZATION_ERROR`.

#### `test_udtf_register_error`
This test verifies that attempting to register an invalid object as a UDTF via `spark.udtf.register` fails appropriately. It first attempts to register a standard UDF (scalar function) and checks that it raises a `PySparkTypeError` with error class `CANNOT_REGISTER_UDTF`. Then, it attempts to register a plain class that lacks the required UDTF structure, expecting the same error behavior.

#### `test_udtf_registration`
This test checks that UDTFs can be registered in the Spark session Catalog and called from SQL queries. It registers a simple addition/subtraction UDTF under the name "testUDTF", executes a SQL query invoking this function with literal arguments, and verifies that the output contains all expected generated rows.

#### `test_udtf_segfault`
This test verifies Spark's behavior when a UDTF triggers a segmentation fault in Python, both in the `eval` and static `analyze` methods. It uses Python's `ctypes` to force a segfault. When `faulthandler` is enabled, it expects the exception message to explicitly mention "Segmentation fault". When disabled, it expects a message suggesting the use of `faulthandler` for better tracebacks.

#### `test_udtf_terminate`
This test demonstrates how the `terminate` method of a UDTF can be used to yield extra summary rows after all inputs have been processed. It creates a UDTF that calculates the count and average of its inputs. `eval` yields rows echoing the input, while `terminate` yields rows with final statistics. It also validates calling the UDTF via SQL lateral join over a range.

#### `test_udtf_terminate_with_additional_args`
This test verifies that the `terminate` method of a UDTF cannot accept any arguments (besides the implicit `self`). It defines a UDTF class with a `terminate` method that expects a positional argument `a`, attempts to call this UDTF, and asserts that a `PythonException` is raised because `terminate` was called without the required argument.

#### `test_udtf_terminate_with_wrong_num_output`
This test ensures that the rows yielded by the `terminate` method must conform to the expected return schema defined by the UDTF decorator. It checks two scenarios: one where `terminate` yields too many columns (3 instead of 2) and one where it yields too few (1 instead of 2). Both result in an expected `UDTF_RETURN_SCHEMA_MISMATCH` failure.

#### `test_udtf_use_large_var_types`
This test checks the behavior of a UDTF when the Spark configuration `spark.sql.execution.arrow.useLargeVarTypes` is set to both `True` and `False`. This configuration determines whether to use large variable types in Arrow execution. The test verifies that the UDTF behaves correctly and produces the expected output in both cases.

#### `test_udtf_with_analyze`
This test validates UDTFs that utilize the static `analyze` method to compute dynamic schemas based on argument properties. The test checks assertions inside the `analyze` method to ensure it receives correct argument metadata (type, literal value existence, non-table, constant expression status). It iterates across multiple queries with different primitive and complex literal types (int, string, array, map, struct), ensuring the dynamic schema matches expectation.

#### `test_udtf_with_analyze_arbitrary_number_arguments`
This test checks UDTFs with a static `analyze` method that accepts a variable number of arguments (`*args`). The `analyze` method creates a dynamic schema with column names derived from argument indices (`col0`, `col1`, etc.) and matching types. The test validates schema resolution and content for calls with 1 argument, 2 arguments, and 0 arguments.

#### `test_udtf_with_analyze_decorator`
This test verifies that a UDTF class defined with both the `@udtf` decorator (with no explicit return type) and a static `analyze` method can be evaluated correctly and registered as a SQL table function. Both Python and SQL invocations are tested and expected to return the hardcoded "hello", "world" tuple under the schema decided by `analyze`.

#### `test_udtf_with_analyze_decorator_parens`
This test functions identically to the previous one, but ensures that calling the decorator with parentheses `@udtf()` without passing parameters behaves correctly alongside an internal `analyze` schema resolution method.

#### `test_udtf_with_analyze_kwargs`
This test checks UDTFs with static `analyze` methods supporting keyword arguments (`**kwargs`). The test performs property checks on the `AnalyzeArgument` values retrieved from kwargs (datatype, literal value, constant expression flag). The test invokes the function using named arguments in both SQL (`a => 10, b => 'x'`) and directly in DataFrame builder Python calls to check that kwarg position sorting works properly.

#### `test_udtf_with_analyze_multiple_arguments`
This test validates UDTF schema analysis when the `analyze` method accepts multiple fixed positional arguments instead of catch-all `*args`. It checks that providing multiple literals dynamically produces a schema matching both input types, validating the behavior via direct API calls and SQL query strings.

#### `test_udtf_with_analyze_non_staticmethod`
This test enforces the rule that the `analyze` method of a UDTF must be static (marked with `@staticmethod`). It checks that attempting to pass a class with an instance method named `analyze` results in a `PySparkAttributeError`. The test also verifies failure conditions when both a return type string is specified in the decorator AND an `analyze` method is present.

#### `test_udtf_with_analyze_null_literal`
This test checks the handling of `None` (null literal) as an input argument to a UDTF that uses an `analyze` method. It verifies that `analyze` receives an argument with `NullType` as its datatype, produces a proper corresponding dataframe schema, and evaluates successfully on the row content producing `None`.

#### `test_udtf_with_analyze_order_by_override_nulls_first`
This test ensures that the `orderBy` constraint returned within the `AnalyzeResult` properly honors its `overrideNullsFirst` property. It iterates through tests with this property set to both `True` and `False`, feeding a two-row dataframe with values 1 and `None`. The test asserts that the result sequence places `None` first when requested, and places `None` last otherwise.

#### `test_udtf_with_analyze_raising_an_exception`
This test checks behavior when a custom error is raised directly inside the `analyze` static method block. It ensures that Spark properly wraps or surfaces the exception as an `AnalysisException` instead of letting it bubble up as an unhandled core Python crash, when a user tries to execute the task.

#### `test_udtf_with_analyze_table_argument`
This test validates passing full Tables as arguments to a UDTF using the SQL `TABLE(...)` constructor wrapper. The `analyze` method validates table properties (no literal scalar value, marked as table, not constant), dynamically extracts the first field type of that table, and yields a derived schema. `eval` filters IDs greater than 5 from the input table rows.

#### `test_udtf_with_analyze_table_argument_adding_columns`
This test checks a UDTF that appends a calculated boolean column to an existing table schema passed as an argument. The UDTF takes a table of IDs, checks for evenness, and yields rows where the new column is `True` for even IDs and `False` for odd IDs, asserting both schema equality and data frame correctness.

#### `test_udtf_with_analyze_table_argument_repeating_rows`
This test demonstrates a multi-argument UDTF accepting both a repetition count integer and a table reference. In `analyze`, it checks that the count is an integer within bounds and table is of the correct type. In `eval`, it duplicates every input row from the table `n` times. The test checks this by providing different integers and asserting failures when non-integers, out-of-range counts, or regular strings instead of tables are supplied.

#### `test_udtf_with_analyze_table_select`
This test verifies that a UDTF can optimize input table processing by restricting which columns are visible to it, via the `select` argument in the `AnalyzeResult`. The test provides a dataframe of two columns but requests only the "id" column in `analyze`. In `eval`, it asserts that the excluded column "value" is missing from the provided row, correctly pruning data access.

#### `test_udtf_with_analyze_taking_keyword_arguments`
This test validates UDTF behavior when the static `analyze` and standard `eval` methods accept generic Python `**kwargs`. It shows that providing no arguments, standard arguments, and named arguments in SQL all yield a static expected two-string dataframe successfully. It checks failure branches ensuring that trying to call this exact kwarg-only function by dumping raw positional arguments results in a `tooManyPositionalArguments` parse error state.

#### `test_udtf_with_analyze_taking_wrong_number_of_arguments`
This test enforces strict mapping between the number of parameters passed to a UDTF vs. what its internal `analyze` static method accepts. The UDTF expects exactly two parameters. The test attempts invoking with 1 argument and with 3 arguments, expecting specific `AnalysisException` errors on both counts.

#### `test_udtf_with_analyze_using_accumulator`
This test delegates execution to its superclass to verify that UDTFs using the `analyze` method can also properly utilize Spark accumulators. It likely ensures accumulator values can be updated within the UDTF lifecycle without breaking schema resolution or evaluation.

#### `test_udtf_with_analyze_using_archive`
This test delegates execution to its superclass to check that UDTFs utilizing the `analyze` method can successfully reference external archive packages shipped over with query context.

#### `test_udtf_with_analyze_using_broadcast`
This test delegates execution to its superclass to verify that UDTFs using the `analyze` method can properly read data values stored inside Spark broadcast variables inside its execution cycle.

#### `test_udtf_with_analyze_using_file`
This test delegates execution to its superclass to verify that UDTFs using the `analyze` method can successfully access static external text files or resources pushed to executors via the Spark context file distribution mechanism.

#### `test_udtf_with_analyze_using_pyfile`
This test verifies that UDTFs can access code modules distributed via Spark's `pyFiles` mechanism during both the static `analyze` phase and standard row evaluation. The test creates a temporary python file returning a column name, calls it to dictate column strings inside the generated schema, and asserts that evaluating and terminating UDTF yields produce the requested behavior.

#### `test_udtf_with_analyze_using_zipped_package`
This test shares the same logical verification purpose as the `pyfile` test above, but enforces the capability of importing directly from a zipped package payload that was shipped across the Spark cluster during the UDTF schema analysis and instance evaluation loops.

#### `test_udtf_with_array_input_type`
This test checks that Spark arrays map cleanly into Python list objects inside UDTFs. It takes an integer array literal `array(1, 2, 3)`, registers a string converter UDTF, and yields the string cast of that list. Because underlying numeric types could be inferred differently (primitive int vs numpy int32 depending on environment), it verifies that output content falls into one of those valid string serializations.

#### `test_udtf_with_array_output_types`
This test checks whether UDTFs can output native lists corresponding to Spark's array storage type. It creates a UDTF returning `array<int>`, takes a single scalar literal integer, and builds a list containing three incrementing numbers. The test checks whether this gets returned correctly as an array element in the outcome row.

#### `test_udtf_with_both_return_type_and_analyze`
This test checks for errors when an operator defines a static `analyze` function on their UDTF class but also attempts to specify a fixed string `returnType` schema string within the `@udtf` decorator simultaneously. The engine enforces that these two schema definition tactics are mutually exclusive and yields a `INVALID_UDTF_BOTH_RETURN_TYPE_AND_ANALYZE` attribute error class.

#### `test_udtf_with_collated_string_types`
This test ensures that UDTFs handle string data types with distinct collation settings cleanly. The UDTF output is declared to carry `UTF8_BINARY`, `UTF8_LCASE`, and `UNICODE` settings. The test executes a `lateralJoin` on a source dataframe that matches this schema, appending an incrementing digit suffix inside the UDTF, and checks that collation schemas survive the projection.

#### `test_udtf_with_conditional_return`
This test demonstrates a UDTF functioning as a dynamic record filter. It sets a condition inside the UDTF's `eval` function that only yields values if they are strictly greater than 5. A lateral join applied onto rows numbered 0-7 filters out all rows except for rows 6 and 7.

#### `test_udtf_with_conditional_return_dataframe`
This test performs precisely the same logical operation as the previous one but compares the behavior using DataFrame API methods like `lateralJoin` and `.outer()` vs. direct raw string SQL calls to ensure operational equivalence between the two engines.

#### `test_udtf_with_empty_output_schema_and_non_empty_output`
This test verifies that a User-Defined Table Function (UDTF) fails with a UDTF_RETURN_SCHEMA_MISMATCH error when its specified return schema is empty (StructType()), but the function actually attempts to yield non-empty data (a tuple with one element (1,)).

#### `test_udtf_with_empty_output_types`
This test checks the behavior of a UDTF with an empty return schema (StructType()) that yields an empty tuple. It asserts that the resulting DataFrame contains a single empty row ([Row()]).

#### `test_udtf_with_empty_yield`
This test verifies that a UDTF with a single output column "a" of type int that yields nothing (an empty yield statement) correctly produces a row with a None value for that column ([Row(a=None)]) when called with a literal value 1.

#### `test_udtf_with_int_and_table_argument_identifier`
This test verifies a UDTF that takes an integer and a table as arguments. The UDTF filters rows from the input table (a temporary view 'v' containing IDs from 0 to 7) where the 'id' is greater than the provided integer (5). It registers the UDTF, creates a temporary view, executes a SQL query using the UDTF with a table identifier as an argument, and asserts that the output contains only rows with 'id' 6 and 7.

#### `test_udtf_with_int_and_table_argument_query`
Similar to the previous test, this one verifies a UDTF taking an integer and a table argument. However, instead of using a view identifier, it passes a SQL query TABLE (SELECT id FROM range(0, 8)) directly as the table argument. It asserts that the UDTF correctly filters rows where 'id' > 5, yielding rows with 'id' 6 and 7.

#### `test_udtf_with_invalid_return_type`
This test ensures that creating a UDTF with an invalid return type specification (a string "int" instead of a proper schema or DDL string describing a struct) raises an InvalidPlanInput exception with a message containing "Invalid.*type".

#### `test_udtf_with_invalid_return_value`
This test verifies that a UDTF fails with a UDTF_RETURN_NOT_ITERABLE error when its eval method returns a non-iterable value (an integer) instead of yielding or returning an iterable of tuples.

#### `test_udtf_with_invalid_return_value_in_terminate`
Similar to the previous test, this one checks the terminate method of a UDTF. It asserts that if terminate returns a non-iterable value (an integer 1), the UDTF execution fails with a UDTF_RETURN_NOT_ITERABLE error.

#### `test_udtf_with_kwargs`
This test verifies that UDTFs support keyword arguments in both SQL and the DataFrame API. It tests successful invocations with named arguments in different orders. It also checks negative cases, ensuring that passing duplicate named arguments or mixing positional and named arguments incorrectly raises appropriate AnalysisException errors.

#### `test_udtf_with_lateral_join`
This test verifies the behavior of a UDTF when used in a SQL LATERAL JOIN. The UDTF takes two integers and yields two rows for each input: one with the sum and one with the difference. The test applies this UDTF to a table with two rows (0, 1) and (1, 2), resulting in a total of 4 rows, and asserts that the output matches the expected DataFrame.

#### `test_udtf_with_lateral_join_dataframe`
This test verifies the DataFrame API's lateralJoin method with UDTFs. It covers several scenarios: joining with outer column references passed positionally and by keyword, joining with a UDTF that takes no arguments (effectively a cross join), and joining with a UDTF that takes a correlated argument based on the input row. All scenarios assert that the result matches the expected output.

#### `test_udtf_with_logging`
This test verifies that logging from within a UDTF works correctly and that the logs can be queried using spark.tvf.python_worker_logs(). It enables Python worker logging, runs a query involving a UDTF that issues warning logs, asserts the query result, and then asserts that the expected log messages with correct levels and contexts are present in the log table.

#### `test_udtf_with_map_input_type`
This test verifies that a UDTF can accept a map type as input. The UDTF converts the input map to its string representation. The test registers the UDTF, calls it with a map created in SQL, and asserts that the stringified output matches the expected Python dict string representation.

#### `test_udtf_with_map_output_types`
This test verifies that a UDTF can return a map type. The UDTF takes an integer and yields a map containing that integer as a key and its string representation as the value. The test asserts that the returned DataFrame contains the expected map object.

#### `test_udtf_with_named_arguments`
This test, similar to a previous one, specifically tests keyword argument support for a UDTF with multiple arguments where only a subset of arguments is relevant to the output. It invokes the UDTF via both SQL and DataFrame API using named arguments in varying orders and verifies the correct result is returned.

#### `test_udtf_with_named_arguments_and_defaults`
This advanced test verifies UDTFs that define a static analyze method to compute return schemas dynamically, and also use named arguments and default parameter values. The analyze method inspects arguments and ensures they match expected types and values, returning the computed schema. The test executes cases with and without the optional 'b' argument and verifies correct execution through both SQL and DataFrame interfaces.

#### `test_udtf_with_named_arguments_lateral_join`
This test verifies the use of keyword arguments for a UDTF within a SQL LATERAL JOIN. The UDTF's schema is resolved dynamically via a static analyze method. The query joins a base relation with the UDTF, referencing columns from the base relation by name in the function arguments, and checks whether the resulting relation matches expectations.

#### `test_udtf_with_named_arguments_negative`
This test reinforces negative validation edge cases for UDTFs using named arguments. It asserts that passing duplicate values for a named parameter, placing positional arguments after named ones, providing arguments not specified in the signature, or passing both positional and named values for the same parameter correctly raise corresponding errors.

#### `test_udtf_with_named_table_arguments`
This test verifies keyword parameter support when passing table references to a UDTF. The UDTF expects a row from a table mapped to the parameter 'a' and accesses its 'id' field. The test covers both SQL and DataFrame API usage with named parameters in various orders and confirms that correct results are produced.

#### `test_udtf_with_neither_return_type_nor_analyze`
This test checks that declaring a UDTF that neither provides a static analyze method nor specifies a returnType argument results in a PySparkAttributeError with error class INVALID_UDTF_RETURN_TYPE when attempting to decorate the class.

#### `test_udtf_with_nested_variant_input`
This test verifies that a UDTF can process inputs containing nested Spark Variant types. It tests three structures: a struct containing a variant, an array of variants, and a map with string keys and variant values. In each case, it queries the UDTF, extracts data from the variant field, and compares the generated result with expected JSON string representations.

#### `test_udtf_with_nested_variant_output`
This test verifies that a UDTF can output structures containing Spark Variant types. Similar to the input test, it covers return types of struct with variant, array of variants, and map of string to variant. The test uses a direct constructor VariantVal with raw bytes to create instances, emits them, translates them to JSON strings on the SQL side, and asserts correctness.

#### `test_udtf_with_no_handler_class`
This test validates that attempting to use the @udtf decorator on non-class types (like a function or a raw integer) triggers a PySparkTypeError with the error code INVALID_UDTF_HANDLER_TYPE, clarifying that UDTF implementations must be classes.

#### `test_udtf_with_non_empty_output_schema_and_empty_output`
This test checks that if a UDTF specifies a schema demanding a non-empty list of columns (like "a: int"), but the execution logic attempts to emit an empty tuple, a PythonException with the message "UDTF_RETURN_SCHEMA_MISMATCH" gets thrown.

#### `test_udtf_with_nondeterministic_input`
This test ensures that UDTF arguments derived from non-deterministic SQL functions (like rand) can still correctly be passed as inputs. It creates a UDTF emitting conditional outputs, evaluates it passing an expression based on a random value seeded to ensure it is always less than 100, and asserts that the resulting dataframe is correct.

#### `test_udtf_with_none_input`
This test makes sure that explicit null or Python None values passed as single parameters to a UDTF are correctly propagated to the UDTF's implementation. It checks both DataFrame API (using lit(None)) and SQL (using null keyword) pathways.

#### `test_udtf_with_none_output`
This test checks that a UDTF can safely yield rows containing None values. It asserts correctness for simple operations as well as for complex scenarios where the UDTF's outputs containing null values are applied in both inner and left join conditions with another static DataFrame.

#### `test_udtf_with_pandas_input_type`
This test documents current edge behavior or a known issue where attempting to use a UDTF that expects Pandas Series objects as input in a simple scalar context errors at runtime because scalar values (integers) are passed instead. It asserts that execution triggers a PythonException related to missing attributes.

#### `test_udtf_with_prepare_string_from_analyze`
This test verifies the ability of a UDTF's static analyze method to pass arbitrary metadata (buffer) to the actual UDTF instance via a custom AnalyzeResult subclass. The analyze method processes input string parameters and returns an object with a custom buffer property that is then accessed by the __init__ constructor of the concrete UDTF instances. The test then checks that counting operations over an input table can successfully emit this buffer string at termination.

#### `test_udtf_with_scalar_analyze_returning_wrong_result`
This test checks that for UDTFs taking scalar arguments, if the analyze method returns invalid structures or includes configurations disallowed in scalar cases (like partitionBy or withSinglePartition), the system throws an AnalysisException containing an appropriate error message mapping back to the problem found.

#### `test_udtf_with_skip_rest_of_input_table_exception`
This test verifies the mechanism for early termination within a UDTF by raising the specific SkipRestOfInputTableException. The UDTF processes an input table row-by-row and raises this exception after it counts 4 rows within a partition. The test demonstrates that this correctly interrupts the processing without causing query failure and supports operations broken into single partitions or customized partitions via SQL commands.

#### `test_udtf_with_struct_input_type`
This test verifies that a UDTF can take struct instances as parameters. The input struct fields are referenced on the passed Row object as properties (like person.name and person.age). The test registers the UDTF and passes a constructed SQL struct to verify exact parity.

#### `test_udtf_with_struct_output_types`
This test demonstrates that a UDTF can emit fields containing structured data by declaring a returnType specifying a struct schema and yielding Python dictionaries holding corresponding keys. The test checks whether this gets properly decoded on the client side.

#### `test_udtf_with_table_analyze_returning_wrong_result`
This test performs negative validation on custom static analyze return types when working specifically with operations receiving tables as inputs. It lists invalid return results (like misplacing ordering columns inside partitioning fields and vice versa) and verifies that proper AnalysisException errors surface at function call sites.

#### `test_udtf_with_table_argument_and_analyze_kwargs`
This test checks that custom analyze methods can inspect parameters passed explicitly as keywords when some of these are table references. It uses assertion locks inside the static analyze block, resolves the returning schemas as maps across the passed parameters, and assesses whether results remain identical across various call configurations.

#### `test_udtf_with_table_argument_and_kwargs`
Similar to the previous test, this one ensures support for processing mixed parameters (where one parameter points towards table references) inside normal keyword-driven UDTFs that specify simple static string return types instead of needing advanced computation blocks.

#### `test_udtf_with_table_argument_and_partition_by`
This test verifies that UDTFs handle table arguments combined with PARTITION BY correctly. It ensures that Spark instantiates separate UDTF object instances per partition and that all inputs routed to one instance share the same partitioning column value. It covers several test cases covering different partitioning keys (including expressions and null values) and cross-validation via lateral joins.

#### `test_udtf_with_table_argument_and_partition_by_and_order_by`
This test extends the partitioning check to ensure that within each partition, rows mapped to a specific UDTF instance are ordered strictly according to defined ORDER BY directions (like ASC or DESC). The test checks that the final resulting output reflects computations expected strictly only on last processed sorted items.

#### `test_udtf_with_table_argument_and_partition_by_no_terminate`
This test ensures that running partitioned queries against table values handed over to an instance of a UDTF lacking explicitly defined terminate sequences still properly yields expected mapped answers resulting straight from in-flight evaluations.

#### `test_udtf_with_table_argument_cte_inside`
This test verifies passing an active SQL CTE block directly as an evaluation property wrapped under a required sub-query instruction for one of the main table-valued attributes targeting a UDTF operation correctly builds executable plans.

#### `test_udtf_with_table_argument_cte_outside`
This test checks that queries containing common table expressions (CTEs) at the outer level can have their resulting alias views correctly referenced inside calls made pointing towards parameters demanding actual TABLE definitions.

#### `test_udtf_with_table_argument_identifier`
Similar to previous tests, this one confirms table identifiers resolved pointing at localized database representations are recognized correctly behind functions designed directly mapping on input tables.

#### `test_udtf_with_table_argument_lateral_join`
This test confirms that queries can run successfully evaluating Lateral Joins that also directly employ pointers specifying passing complete localized relation schemas as parameters behind the called UDTF instances.

#### `test_udtf_with_table_argument_malformed_query`
This simple negative validation verifies that passing references in function sub-expressions leading towards non-existent or inaccessible objects produces appropriate AnalysisException alerts indicating missing resources.

#### `test_udtf_with_table_argument_multiple`
This test covers conditions where a UDTF implementation claims and expects more than one complete relation passed by argument. It verifies that trying to pass two tables raises detailed AnalysisException errors when the related feature configuration flag sits disabled. Toggling back the related configuration key allows successful extraction, which is properly affirmed through result assertions.

#### `test_udtf_with_table_argument_query`
This test simply demonstrates that pass-through behavior expected from the defined udtf_for_table_argument returns proper values mapped by row numbers.

#### `test_udtf_with_table_argument_unknown_identifier`
Similar to code blocks appearing in previous tests, this verifies behavior responding against unavailable referenced table resources resulting directly in SQL exceptions.

#### `test_udtf_with_table_argument_with_partition_by_and_order_by_from_analyze`
This test verifies the execution of a UDTF where partitioning and ordering demands are inferred directly by processing static answers routed through returned definitions stored on instances of AnalyzeResult. It verifies partitioned routing and strict sorting keys and ensures standard queries reflect correctness against aggregated and emitted values.

#### `test_udtf_with_table_argument_with_single_partition`
This test enforces rules governing executions running behind WITH SINGLE PARTITION blocks declared on incoming relations. It affirms that data ordering continues working alongside properly targeted single instances where everything must ultimately combine.

#### `test_udtf_with_table_argument_with_single_partition_from_analyze`
This final test in the sequence affirms the behavior shown in the previous test by verifying that single-partition behavior remains properly forced when its settings are dictated during resolution tasks performed in standard analyze methods.

#### `test_udtf_with_variant_input`
Verifies that a user-defined table function (UDTF) can accept a variant input. The test registers a UDTF that parses JSON input and returns its string representation. It checks that the output matches the expected JSON string for 10 rows.

#### `test_udtf_with_variant_output`
Tests a UDTF that outputs variant values. It constructs specific variant values in Python and verifies that they are correctly returned and translated to JSON on the Spark server side.

#### `test_udtf_with_wrong_num_input`
Ensures that calling a UDTF with the incorrect number of arguments (either missing required arguments or providing too many positional arguments) raises a PythonException with specific expected error messages.

#### `test_udtf_with_wrong_num_output`
Verifies that returning a different number of columns from a UDTF than specified in its return schema raises a PythonException with a 'UDTF_RETURN_SCHEMA_MISMATCH' error code. It tests both returning fewer columns and more columns.

#### `test_udtf_with_zero_arg_and_invalid_return_value`
Tests that returning a non-iterable value from a UDTF's eval method raises a PythonException with a 'UDTF_RETURN_NOT_ITERABLE' error code.

#### `test_udtf_yield_multi_cols`
Tests a basic UDTF that returns a single row with multiple columns. It verifies the results of mapping an input to multiple outputs.

#### `test_udtf_yield_multi_row_col`
Tests a UDTF that generates multiple rows, each with multiple columns, based on its inputs. It asserts correctness on a static set of expected rows.

#### `test_udtf_yield_multi_rows`
Verifies that a UDTF can return multiple rows for a single input row. The test yields two rows for every input.

#### `test_udtf_yield_single_row_col`
Validates that a UDTF can yield a single row with a single column, asserting the output value correctly matches the processed input.

### Class: `LegacyArrowUDTFParityTests`

#### `test_array_output_type_casting`
This test, specific to Arrow-optimized UDTFs, examines how an array output from Python (`[0, 1.1, 2]`) is cast to various Spark return types defined in the schema. It iterates through a list of target return types and expected results, showing that in many cases of mismatch it returns nulls or handles element-wise casting (e.g., to `array<string>`), while certain date/timestamp conversions fail completely with an `AttributeError`.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_df_asTable`
This test verifies that the DataFrame API's `df.asTable()` method produces an object that can be passed directly to a UDTF as a table argument, matching the behavior of the SQL `TABLE()` syntax.

#### `test_df_asTable_chaining_methods`
This test checks the fluent interface on table objects returned by `asTable()`. It verifies that you can correctly chain methods like `partitionBy()`, `orderBy()`, and `withSinglePartition()` to control processing in the UDTF call. It tests the row ordering and checks that trying to use these builders in illegal sequences or combinations results in an `IllegalArgumentException` thrown by the Spark parser.

#### `test_docstring`
This test ensures that when a class is wrapped as a UDTF, the docstrings on the original class and its special methods (`__init__`, `analyze`, `eval`, `terminate`) are preserved on the wrapper function so that Python help tools and auto-documentation continue to work properly.

#### `test_eval_type`
This test verifies that the internal `evalType` property on the generated UDTF object is correctly set based on whether Arrow optimization was enabled. It expects `SQL_TABLE_UDF` when Arrow is disabled and `SQL_ARROW_TABLE_UDF` when Arrow is enabled.

#### `test_eval_with_exception`
This test verifies that if the `eval` method of a UDTF raises a standard Python exception, it gets translated by the runner into a `PythonException` containing the Spark error class `UDTF_EXEC_ERROR` and specific diagnostic text calling out that it happened in the 'eval' method.

#### `test_inconsistent_output_types`
This test documents Arrow UDTF behavior when the UDTF yields Python objects of different types across rows (e.g., an integer then a list) in a way that doesn't perfectly match the fixed return schema. Depending on what is specified in `returnType`, it shows that Arrow handling will result in passing through the correctly matching elements and returning nulls for the mismatched ones instead of abruptly failing.

#### `test_init_with_exception`
Similar to the eval failure test, this ensures that any exception raised inside the UDTF's `__init__` constructor correctly results in a `PythonException` marked as `UDTF_EXEC_ERROR` specifying that the error occurred during the initialization step.

#### `test_map_output_type_casting`
Similar to the array casting test, this exercises Arrow UDTF behavior when mapping a Python dictionary output to various Spark target schema types. It demonstrates which structures map cleanly (like converting dictionary values to strings in `map<string,string>`) and which ones drop data to null due to cast failures or throw explicit `AttributeError`s for dates and timestamps.

#### `test_nondeterministic_udtf`
This test verifies that a UDTF containing non-deterministic code (here, using Python's `random.random()`) can be registered and called successfully. Because `random.random()` returns a value between 0 and 1, casting that float to an int always results in 0, effectively making the behavior deterministic in this specific test's assertions, which assume the UDTF simply outputs the input integer unchanged.

#### `test_numeric_output_type_casting`
This test verifies how a User-Defined Table Function (UDTF) handles yielding a numeric value (specifically the integer 1) when the expected return schema specifies different data types. It tests casting from integer to various types including booleans, other numeric types, strings, dates, timestamps, and complex types like arrays and maps. It checks that correct values are produced where casting is valid and that appropriate exceptions (like AttributeError for date/timestamp where int lacks necessary methods) are raised where invalid.

#### `test_numeric_string_output_type_casting`
This test verifies the behavior of a UDTF when it yields a string representation of a number ("1") and the return schema expects other types. It ensures that casting from string to most numeric types results in None (as per Spark's casting rules in this context) and that string to string works, and string to binary produces a bytearray. It also checks that invalid conversions to date and timestamp raise AttributeError.

#### `test_simple_udtf`
A basic smoke test for UDTFs. It defines a simple UDTF class that yields a single row with two string columns ("hello", "world") and verifies that invoking this UDTF returns a DataFrame with the expected content and schema.

#### `test_simple_udtf_with_analyze`
Verifies that a UDTF can use a static analyze method to dynamically determine its output schema instead of providing a hardcoded schema string to the @udtf decorator. It tests that both the Python API and Spark SQL can correctly use this UDTF to produce a DataFrame with the schema specified by analyze.

#### `test_string_output_type_casting`
Tests how a UDTF handles yielding a non-numeric string ("hello") against various requested output types. It confirms that invalid conversions to numeric types result in None values, while string to string succeeds, and string to binary yields the string's bytes.

#### `test_struct_output_type_casting_dict`
Verifies the behavior when a UDTF yields a Python dictionary representing a structure. It tests casting this dictionary to various Spark types, including a string representation of the map, a map type with string keys and values, and struct types with various field types (string, int, float). It confirms that fields are correctly cast or set to None if casting fails (e.g., trying to put float 1.1 into an int field).

#### `test_struct_output_type_casting_row`
This test calls a helper method check_struct_output_type_casting_row and passes PickleException as the expected failure. It appears to verify that yielding a Row object (or similar struct-like object) that cannot be properly serialized or cast raises a PickleException in the legacy Arrow UDTF implementation.

#### `test_terminate_with_exceptions`
Verifies that if an exception is raised within the terminate method of a UDTF, it is properly captured and wrapped in a PythonException with a specific error class UDTF_EXEC_ERROR by Spark, ensuring that errors during the finalization phase of the UDTF are correctly reported to the user.

#### `test_udtf_access_spark_session`
This test inherits and runs the parent class's test for checking if a UDTF can access the active Spark session during its execution. It verifies that the UDTF can successfully interact with the session context.

#### `test_udtf_access_spark_session_connect`
Verifies that in the Spark Connect environment, a UDTF cannot access the active Spark session directly within its eval method to perform operations like df.collect(). It expects a PythonException indicating NO_ACTIVE_SESSION, which is expected behavior as UDTF execution happens on workers isolated from the master session.

#### `test_udtf_analyze_with_logging`
Tests that standard Python logging used within the analyze static method of a UDTF is correctly captured and made available via the spark.tvf.python_worker_logs() table valued function. It verifies that the log level, message, and context (class name and function name) are correctly recorded when worker logging is enabled.

#### `test_udtf_analyze_with_pyspark_logger`
Similar to the previous test, but specifically verifies that using PySparkLogger instead of standard Python logging within a UDTF's analyze method also works correctly, capturing logs and making them available via python_worker_logs with additional context like data types.

#### `test_udtf_arrow_sql_conf`
Verifies that the configuration setting spark.sql.execution.pythonUDTF.arrow.enabled correctly controls whether UDTFs are executed using Arrow optimization. It checks that the internal evalType of the UDTF correctly reflects whether it is standard table UDF or an Arrow-based table UDF depending on this config.

#### `test_udtf_binary_type`
Verifies how binary inputs are passed to a UDTF based on the spark.sql.execution.pyspark.binaryAsBytes configuration. When true, it expects binary data to be passed as standard Python bytes objects. When false, it expects it as bytearray objects.

#### `test_udtf_cleanup_with_exception_in_eval`
Tests the lifecycle management of UDTFs when an error occurs during the eval phase. It verifies that even if eval fails with an exception, the cleanup method is guaranteed to run to allow resource cleanup, but the terminate method (which implies successful completion) is bypassed.

#### `test_udtf_cleanup_with_exception_in_terminate`
Similar to the previous test, this verifies that if the terminate method of a UDTF raises an exception, the cleanup method is still executed to ensure resources are cleaned up properly.

#### `test_udtf_decorator`
A simple test ensuring that the @udtf decorator works as expected when defining a UDTF with a specified return type, and that it can be invoked to produce correct results.

#### `test_udtf_determinism`
Verifies that UDTFs are marked as non-deterministic by default in Spark, and that calling .asDeterministic() on the UDTF object correctly marks it as deterministic.

#### `test_udtf_eval_returning_non_tuple`
This test verifies that the eval method of a UDTF must yield tuples representing rows, and that attempting to yield other types like raw integers, or using return statements instead of yield with non-tuple outputs, correctly raises PythonException with the error class UDTF_INVALID_OUTPUT_ROW_TYPE.

#### `test_udtf_eval_returning_tuple_with_struct_type`
Verifies that when a UDTF's return schema contains a struct type, the eval method must yield a nested tuple structure to map correctly to the struct's fields. It tests that yielding the correct structure succeeds, while yielding a flat tuple when a struct is expected correctly raises a UDTF_RETURN_SCHEMA_MISMATCH error.

#### `test_udtf_eval_returning_udt`
Verifies that UDTFs can correctly yield custom User-Defined Types (UDTs). It defines a UDTF that produces an ExamplePoint object within its output tuple and asserts that the resulting DataFrame contains the expected UDT data.

#### `test_udtf_eval_taking_udt`
Verifies that a UDTF can accept a custom User-Defined Type (UDT) as an input argument in its eval method. It creates a DataFrame with a UDT column, performs a lateral join with a UDTF that operates on that UDT, and validates the results.

#### `test_udtf_eval_with_no_return`
Confirms that if a UDTF's eval method does not yield or return any rows (i.e., it is empty or has a bare return statement), the result is an empty DataFrame rather than an error.

#### `test_udtf_eval_with_return_stmt`
Verifies that instead of using yield, a UDTF's eval method can return a list of tuples all at once, and Spark will correctly interpret this as the set of rows generated by that evaluation call.

#### `test_udtf_init`
Verifies that the standard Python __init__ constructor on a UDTF class works as expected, allowing for state initialization that can then be referenced during execution in the eval method.

#### `test_udtf_init_with_additional_args`
Verifies that UDTF classes cannot have additional required arguments in their __init__ constructor. Spark expects only the default self argument, and passing additional ones raises an error indicating the constructor cannot accept them.

#### `test_udtf_kill_on_timeout`
Verifies that Spark can actively terminate a Python worker process if a UDTF's execution (either in eval or analyze) takes longer than a configured idle timeout limit. It sets a 1-second timeout, forces a 2-second sleep, and asserts that the process is killed as expected with a timeout error.

#### `test_udtf_no_eval`
Verifies that attempting to create a UDTF from a class that lacks the mandatory eval method triggers a PySparkAttributeError with error class INVALID_UDTF_NO_EVAL.

#### `test_udtf_nullable_check`
A comprehensive test verifying that nullability constraints defined in the UDTF's return schema are respected. It attempts to yield data containing nulls in arrays, map keys/values, and struct fields marked as non-nullable, and verifies that appropriate PySparkRuntimeError exceptions are raised in those cases, while allowing nulls when the schema permits them.

#### `test_udtf_pickle_error`
Verifies that if a UDTF references a non-serializable (non-picklable) Python object (like an open file object) in its closure, it fails to be shipped to workers, and raises a specific PySparkPicklingError with error class UDTF_SERIALIZATION_ERROR.

#### `test_udtf_register_error`
Verifies that the UDTF registration API throws distinct errors when users attempt invalid registrations, such as trying to register a regular function (UDF) or a class that hasn't been decorated or constructed as a UDTF, using error class CANNOT_REGISTER_UDTF.

#### `test_udtf_registration`
Verifies that a UDTF can be registered under a specific name in the session and subsequently queried using Spark SQL, yielding correct multi-row results.

#### `test_udtf_segfault`
Tests the behavior of Spark workers when a UDTF triggers a low-level segmentation fault (simulated here using ctypes). It verifies that if faulthandler is enabled in Spark configuration, a stack trace is printed to the console, and if not, it suggests enabling it for better debugging.

#### `test_udtf_terminate`
Verifies that the terminate method on a UDTF can yield rows after all input processing is done. It sets up a UDTF that calculates a running count and sum in eval and then yields the count and average in terminate, and validates that these final rows are appended to the output DataFrame.

#### `test_udtf_terminate_with_additional_args`
Verifies that the terminate method of a UDTF cannot accept any arguments beyond the default self. If additional arguments are defined on terminate, invoking it results in a Python error.

#### `test_udtf_terminate_with_wrong_num_output`
Verifies that if rows yielded by the terminate method of a UDTF do not conform to the length of the expected output schema (either too many or too few columns), Spark will correctly raise a UDTF_RETURN_SCHEMA_MISMATCH exception.

#### `test_udtf_use_large_var_types`
Verifies that the spark.sql.execution.arrow.useLargeVarTypes configuration can be successfully toggled when running UDTFs with Arrow execution enabled, ensuring that larger variable types in Arrow are handled correctly when enabled without affecting the correctness of the generated results.

#### `test_udtf_with_analyze`
A detailed test for the UDTF analyze static method. It passes literal values of various types (int, string, array, map, struct) to the UDTF and verifies that the analyze method receives correct argument metadata (type, value, whether it's a table, etc.) allowing it to return a dynamically constructed schema that exactly matches the input argument's type.

#### `test_udtf_with_analyze_arbitrary_number_arguments`
Verifies that a UDTF's analyze static method can accept a variable number of arguments (*args) and correctly construct a return schema based on the types of all provided arguments dynamically.

#### `test_udtf_with_analyze_decorator`
Verifies that the @udtf decorator correctly identifies and utilizes a static analyze method defined inside a class to infer the schema, without the user needing to provide an explicit returnType argument to the decorator itself.

#### `test_udtf_with_analyze_decorator_parens`
Verifies that applying the @udtf() decorator with empty parentheses still correctly detects and uses the class's static analyze method for schema resolution.

#### `test_udtf_with_analyze_kwargs`
Verifies that the analyze static method of a UDTF can accept keyword arguments (**kwargs) and dynamically generate a return schema based on the provided named arguments and their data types.

#### `test_udtf_with_analyze_multiple_arguments`
Verifies that a UDTF's analyze method can accept multiple specific positional arguments and construct a corresponding output schema reflecting the types of those arguments.

#### `test_udtf_with_analyze_non_staticmethod`
Verifies that Spark requires the analyze method of a UDTF to be declared as a @staticmethod. Defining it as a standard instance method triggers a PySparkAttributeError during UDTF creation.

#### `test_udtf_with_analyze_null_literal`
Verifies that when a UDTF with an analyze method is called with a SQL NULL literal, the analyze method correctly receives this argument and can generate a schema that includes a NullType column.

#### `test_udtf_with_analyze_order_by_override_nulls_first`
Verifies that the analyze method can specify sort ordering constraints for the input data in its return AnalyzeResult, including overriding whether nulls appear first or last during that sort, and that Spark correctly respects this ordering.

#### `test_udtf_with_analyze_raising_an_exception`
Verifies that if an unhandled exception is raised during execution of the static analyze method of a UDTF, it is captured by Spark and surfaced to the user as an AnalysisException.

#### `test_udtf_with_analyze_table_argument`
Verifies that a UDTF can accept whole tables as arguments using the SQL TABLE keyword. It confirms that the analyze method correctly identifies the argument as a table, has access to the full input table's schema, and can compute a resulting schema based on that input.

#### `test_udtf_with_analyze_table_argument_adding_columns`
Verifies that a UDTF can take a table as an argument and dynamically compute its own output schema by simply appending new columns onto the schema of the provided input table, and that the UDTF can yield rows containing both the original and new columns.

#### `test_udtf_with_analyze_table_argument_repeating_rows`
A more complex test verifying a UDTF that takes a scalar integer and a table as arguments. It validates that the analyze method can execute arbitrary Python validation logic on the input arguments (such as verifying a range constraint on the scalar argument), can assert that the second argument is a table, and that the eval method successfully repeats output rows the specified number of times. It also tests various failure scenarios with invalid arguments.

#### `test_udtf_with_analyze_table_select`
This test defines a User-Defined Table Function (UDTF) with an `analyze` method that returns an `AnalyzeResult` specifying that only the 'id' column should be selected from the input table. The `eval` method asserts that the 'value' column is NOT present in the input row, confirming that the `select` projection worked. It then yields the 'id'. The test creates a DataFrame with 'id' and 'value' columns, calls the UDTF passing the DataFrame as a table, and asserts that the output contains only the 'id' values.

#### `test_udtf_with_analyze_taking_keyword_arguments`
This test verifies UDTFs where the `analyze` method takes keyword arguments (`**kwargs`). It registers the UDTF and tests that it can be called with no arguments and with named arguments. It also asserts that passing positional arguments when only keyword arguments are supported leads to an `AnalysisException`.

#### `test_udtf_with_analyze_taking_wrong_number_of_arguments`
This test defines a UDTF where the `analyze` method expects exactly two arguments. It then verifies that calling the UDTF with one argument or three arguments raises an `AnalysisException` due to argument mismatch.

#### `test_udtf_with_analyze_using_accumulator`
This test verifies that a UDTF can use an accumulator within its `analyze` method by calling the implementation in the superclass `UDTFParityTests`. This is likely a parity test ensuring that the feature works in the current context by reusing the base test logic.

#### `test_udtf_with_analyze_using_archive`
This test verifies that a UDTF can use an archive file within its `analyze` method by calling a helper method `check_udtf_with_analyze_using_archive` in the superclass `UDTFParityTests`. This ensures feature parity by reusing established test logic.

#### `test_udtf_with_analyze_using_broadcast`
This test verifies that a UDTF can use a broadcast variable within its `analyze` method by calling the implementation in the superclass `UDTFParityTests`. This ensures feature parity by reusing base test logic.

#### `test_udtf_with_analyze_using_file`
This test verifies that a UDTF can use a file within its `analyze` method by calling a helper method `check_udtf_with_analyze_using_file` in the superclass `UDTFParityTests`. This ensures feature parity by reusing base test logic.

#### `test_udtf_with_analyze_using_pyfile`
This test verifies that a UDTF can access and use a Python file added via `self._add_pyfile`. It creates a temporary Python file with a simple function, adds it to the Spark session, and then defines a UDTF that imports and calls that function within its `analyze`, `eval`, and `terminate` methods. It checks that the output schema and data are correct, demonstrating that the added file is available in all phases of the UDTF lifecycle.

#### `test_udtf_with_analyze_using_zipped_package`
Similar to the previous test, this one verifies that a UDTF can use a zipped Python package added to the Spark session. It creates a temporary directory, writes an `__init__.py` file, zips it, adds it to the session, and verifies that the UDTF can import and use the package in `analyze`, `eval`, and `terminate`.

#### `test_udtf_with_array_input_type`
This test verifies that a UDTF can accept an array as input. It defines a UDTF that takes an argument, converts it to a string, and yields it. It calls the UDTF with an array constructed in SQL and asserts that the output string representation matches the expected array format (either standard Python list or numpy representation).

#### `test_udtf_with_array_output_types`
This test verifies that a UDTF can yield arrays as output. It defines a UDTF that takes an integer and yields an array containing `[x, x+1, x+2]`. It asserts that the returned DataFrame has the expected array content.

#### `test_udtf_with_both_return_type_and_analyze`
This test verifies that specifying both a `returnType` in the `@udtf` decorator and an `analyze` method in the UDTF class is invalid and raises a `PySparkAttributeError` with a specific error class `INVALID_UDTF_BOTH_RETURN_TYPE_AND_ANALYZE`.

#### `test_udtf_with_collated_string_types`
This test verifies that UDTFs handle collated string types correctly. It defines a UDTF that takes four string arguments, each with a different collation (default, UTF8_BINARY, UTF8_LCASE, UNICODE) specified in the `returnType`. It performs a lateral join with this UDTF and verifies both the resulting data and the schema of the output, ensuring the collation information is preserved in the output fields.

#### `test_udtf_with_conditional_return`
This test verifies that a UDTF can conditionally yield rows. It defines a UDTF that only yields the input value if it is greater than 5. It uses this UDTF in a lateral join in SQL against a range of numbers and asserts that only rows meeting the condition are returned.

#### `test_udtf_with_conditional_return_dataframe`
Similar to the previous test, this one verifies conditional return using the DataFrame API `lateralJoin` instead of pure SQL. It compares the result with the SQL equivalent to ensure parity.

#### `test_udtf_with_empty_output_schema_and_non_empty_output`
This test verifies that if a UDTF is declared with an empty return schema (empty `StructType`) but its `eval` method yields a non-empty tuple, a `PythonException` with error class `UDTF_RETURN_SCHEMA_MISMATCH` is raised.

#### `test_udtf_with_empty_output_types`
This test verifies that a UDTF can be defined with an empty return schema and yield empty tuples. It asserts that this execution produces a DataFrame containing empty rows.

#### `test_udtf_with_empty_yield`
This test verifies the behavior when a UDTF yields nothing (just a bare `yield` statement). It asserts that this results in a row with all output columns set to `None`.

#### `test_udtf_with_int_and_table_argument_identifier`
This test verifies a UDTF that takes both an integer and a table as arguments. It creates a temporary view, calls the UDTF with a constant integer and the table reference in SQL, and asserts that the UDTF correctly filters the table rows based on the integer argument.

#### `test_udtf_with_int_and_table_argument_query`
Similar to the previous test, this one verifies a UDTF taking an integer and a table argument, but the table argument is provided as a subquery (`TABLE (SELECT id FROM range(0, 8))`) instead of a named view identifier.

#### `test_udtf_with_invalid_return_type`
This test verifies that specifying an invalid return type (like 'int' instead of a struct or DDL string describing a struct) for a UDTF raises an `InvalidPlanInput` exception when attempting to collect results.

#### `test_udtf_with_invalid_return_value`
This test verifies that if the `eval` method of a UDTF returns a value instead of yielding or returning an iterable, a `PythonException` with error class `UDTF_RETURN_NOT_ITERABLE` is raised.

#### `test_udtf_with_invalid_return_value_in_terminate`
Similar to the previous test, this one verifies that if the `terminate` method of a UDTF returns a non-iterable value, a `PythonException` with error class `UDTF_RETURN_NOT_ITERABLE` is raised.

#### `test_udtf_with_kwargs`
This test verifies that UDTFs handle keyword arguments correctly. It checks that arguments can be passed by name in both SQL and the DataFrame API, and in different orders. It also checks negative cases where duplicate named arguments or positional arguments after named arguments are used, raising `AnalysisException`.

#### `test_udtf_with_lateral_join`
This test verifies a basic lateral join with a UDTF in SQL. The UDTF takes two integers and yields their sum and difference. The test verifies that the lateral join produces the expected cross-product-like result for each input row.

#### `test_udtf_with_lateral_join_dataframe`
This test verifies lateral joins with UDTFs using the DataFrame API `lateralJoin` method. It covers various ways of passing arguments (positional, named, different order) and also tests UDTFs with no arguments and UDTFs that generate sequences based on input arguments.

#### `test_udtf_with_logging`
This test verifies that logging within a UDTF works correctly and that logs are propagated to the driver. It enables worker logging, executes a UDTF that logs a warning, and then checks that the warning message is present in the logs retrieved via `tvf.python_worker_logs()`.

#### `test_udtf_with_map_input_type`
This test verifies that a UDTF can accept a map type as input. It passes a map constructed in SQL to the UDTF and asserts that the string representation of the received argument matches the expected map format.

#### `test_udtf_with_map_output_types`
This test verifies that a UDTF can yield maps as output. It asserts that the returned DataFrame contains the expected map content.

#### `test_udtf_with_named_arguments`
This test is similar to Test 24 but focuses on a simpler UDTF and verifies that named arguments work correctly in SQL and DataFrame API without negative testing.

#### `test_udtf_with_named_arguments_and_defaults`
This test verifies UDTFs with named arguments and default values in both `analyze` and `eval` methods. The `analyze` method performs assertions on the arguments it receives. The test covers cases with and without the optional argument 'b' provided, in both SQL and DataFrame API.

#### `test_udtf_with_named_arguments_lateral_join`
This test verifies that named arguments can be used when calling a UDTF within a lateral join in SQL.

#### `test_udtf_with_named_arguments_negative`
This test focuses on negative cases for named arguments in UDTFs. It checks for duplicate named arguments, positional arguments after named arguments, missing required arguments, and multiple values provided for the same argument.

#### `test_udtf_with_named_table_arguments`
This test verifies that table arguments can be passed to UDTFs as named arguments in both SQL and DataFrame API.

#### `test_udtf_with_neither_return_type_nor_analyze`
This test verifies that defining a UDTF without either a `returnType` or an `analyze` method is invalid and raises a `PySparkAttributeError` with error class `INVALID_UDTF_RETURN_TYPE`.

#### `test_udtf_with_nested_variant_input`
This test verifies that UDTFs can accept nested structures containing Variant types (struct with variant, array of variant, map with variant) as input and correctly access the variant content using `toJson()`.

#### `test_udtf_with_nested_variant_output`
This test verifies that UDTFs can yield nested structures containing Variant types as output. It constructs `VariantVal` objects manually and asserts that the JSON representation of the output matches the expected result.

#### `test_udtf_with_no_handler_class`
This test verifies that creating a UDTF with something other than a class (like a function or an integer) as the handler raises a `PySparkTypeError` with error class `INVALID_UDTF_HANDLER_TYPE`.

#### `test_udtf_with_non_empty_output_schema_and_empty_output`
This test verifies that if a UDTF is declared with a non-empty return schema but its `eval` method yields an empty tuple, a `PythonException` with error class `UDTF_RETURN_SCHEMA_MISMATCH` is raised.

#### `test_udtf_with_nondeterministic_input`
This test verifies that a UDTF can accept non-deterministic expressions (like `rand()`) as input arguments.

#### `test_udtf_with_none_input`
This test verifies that a UDTF can accept `None` (or SQL `NULL`) as input and handles it correctly.

#### `test_udtf_with_none_output`
This test verifies that a UDTF can yield rows containing `None` values and that they are correctly handled in joins (inner and left joins are tested).

#### `test_udtf_with_pandas_input_type`
This test attempts to use a UDTF with `pandas.Series` as input types. It expects a `PythonException` at runtime because the regular UDTF does not support pandas series as arguments directly in this context.

#### `test_udtf_with_prepare_string_from_analyze`
This test verifies that a custom attribute ('buffer') added to a subclass of `AnalyzeResult` in the `analyze` method can be accessed in the UDTF's `__init__` method. This allows passing state from the analysis phase to the execution phase.

#### `test_udtf_with_scalar_analyze_returning_wrong_result`
This test verifies that returning invalid objects from the `analyze` method (like a string, or an `AnalyzeResult` with inappropriate settings for a non-table UDTF) raises `AnalysisException`.

#### `test_udtf_with_skip_rest_of_input_table_exception`
This test verifies that raising a `SkipRestOfInputTableException` within a UDTF's `eval` method correctly stops further processing of rows from the input table for that partition, without failing the query.

#### `test_udtf_with_struct_input_type`
This test verifies that a UDTF can accept a struct as input and access its fields.

#### `test_udtf_with_struct_output_types`
This test verifies that a UDTF can yield rows containing struct fields.

#### `test_udtf_with_table_analyze_returning_wrong_result`
Similar to Test 45, this test verifies that returning invalid `AnalyzeResult` objects from the `analyze` method when the UDTF takes table arguments raises `AnalysisException`. It covers invalid combinations of partitioning and ordering columns.

#### `test_udtf_with_table_argument_and_analyze_kwargs`
This test verifies UDTFs that take both table arguments and scalar arguments, and where the `analyze` method receives them as keyword arguments. It checks that the `analyze` method can inspect the metadata of both table and scalar arguments.

#### `test_udtf_with_table_argument_and_kwargs`
This test verifies that a User-Defined Table Function (UDTF) can accept a table argument and keyword arguments (kwargs) together. It defines a UDTF that takes kwargs, extracts the 'id' from table 'a' and the value of 'b', and yields them. It registers the UDTF and tests it with four different invocations (two SQL queries with named arguments in different orders, and two programmatic invocations using `asTable()`). It asserts that all invocations return the expected rows where 'a' is the range value and 'b' is 'x'.

#### `test_udtf_with_table_argument_and_partition_by`
This test validates the behavior of a UDTF when used with a TABLE argument and a PARTITION BY clause. It defines a UDTF that sums an 'input' column for each partition defined by a 'partition_col'. It asserts that within each instance of the UDTF (per partition), all rows have the same partitioning column value. It tests this with both basic examples and cases with constant values (including NULL). It also combines a lateral join with a TABLE argument and PARTITION BY, verifying that the partitioning logic still holds correct counts and values.

#### `test_udtf_with_table_argument_and_partition_by_and_order_by`
This test verifies UDTFs processing a TABLE argument with both PARTITION BY and ORDER BY clauses. It creates a UDTF that tracks the last value seen in the 'input' column for each partition. The test ensures that rows arrive in the order specified by the ORDER BY clause (ascending or descending) within each partition, and that the UDTF correctly identifies the last value based on that ordering. It tests variations of ORDER BY (asc, desc, with expressions) and both SQL and programmatic table arguments.

#### `test_udtf_with_table_argument_and_partition_by_no_terminate`
This test checks the behavior of a UDTF with a TABLE argument and PARTITION BY clause when the UDTF class does not have a `terminate` method. It uses a UDTF created by a helper method, registers it, and executes a query with a table argument partitioned by id. It verifies that the output matches expected rows, showing that the UDTF still executes correctly without `terminate`.

#### `test_udtf_with_table_argument_cte_inside`
This test verifies that a UDTF can take a TABLE argument where the query inside the `TABLE()` operator contains a Common Table Expression (CTE). It registers a UDTF and runs a query where the table argument is defined by a CTE ('t' from a range of 8). It asserts that the result matches the expected output of the UDTF.

#### `test_udtf_with_table_argument_cte_outside`
This test confirms that a UDTF can accept a TABLE argument defined by a CTE that is declared outside the UDTF call. It tests two SQL query structures: one referencing the CTE in a SELECT within `TABLE()`, and another referencing the CTE directly by name inside `TABLE()`. Both cases are verified to return the correct rows.

#### `test_udtf_with_table_argument_identifier`
This test ensures that a UDTF can be called with a TABLE argument referencing a temporary view identifier. It creates a temporary view 'v' containing a range of IDs, and calls the UDTF with `TABLE(v)`. It verifies that the UDTF processes the view content correctly and produces the expected output.

#### `test_udtf_with_table_argument_lateral_join`
This test validates the use of a UDTF in a lateral join where the UDTF takes a table argument referencing a column/table from the preceding part of the join. It calls the UDTF with `TABLE(t)` where 't' is the alias for `range(0, 8)`. It checks that the output is correct.

#### `test_udtf_with_table_argument_malformed_query`
This test verifies that an appropriate error is raised when a UDTF is called with a TABLE argument containing a query that references a non-existent table or view. It asserts that an `AnalysisException` with the message "TABLE_OR_VIEW_NOT_FOUND" is thrown.

#### `test_udtf_with_table_argument_multiple`
*No description available.*

#### `test_udtf_with_table_argument_query`
This is a basic test verifying that a UDTF can take a TABLE argument defined by a subquery. It calls the UDTF with a table derived from `SELECT id FROM range(0, 8)` and asserts the expected result.

#### `test_udtf_with_table_argument_unknown_identifier`
This test ensures that if a UDTF is called with a TABLE argument referencing an unknown identifier (like a view that doesn't exist), an `AnalysisException` with the message "TABLE_OR_VIEW_NOT_FOUND" is raised.

#### `test_udtf_with_table_argument_with_partition_by_and_order_by_from_analyze`
This test verifies a UDTF where the partitioning and ordering requirements are specified by the UDTF's static `analyze` method instead of the SQL query itself. The `analyze` method returns an `AnalyzeResult` specifying `partitionBy` on 'partition_col' and `orderBy` on 'input'. The test creates a UDTF that asserts these ordering and partitioning constraints hold true during execution, and yields the aggregate results in `terminate`. It tests this behavior by passing a simple table without explicit partitioning in the SQL query, verifying that Spark applies the UDTF's requested execution distribution correctly.

#### `test_udtf_with_table_argument_with_single_partition`
This test validates calling a UDTF with the `WITH SINGLE PARTITION` clause. This tells Spark to process all input rows on a single worker instance (partition) instead of distributing them. The UDTF asserts that rows are delivered in the order specified by the `ORDER BY` clause. The test verifies that all 40 rows are accounted for and summed correctly in a single result row.

#### `test_udtf_with_table_argument_with_single_partition_from_analyze`
This test mirrors test 12 but for the `withSinglePartition` property. The UDTF's `analyze` method specifies `withSinglePartition=True` and an ordering. The test confirms that calling the UDTF in SQL without explicit distribution instructions still results in a single-partition ordered execution as specified by the UDTF's custom plan analyzed at compile-time.

#### `test_udtf_with_variant_input`
This test confirms that a UDTF can accept values of type Variant. It passes a parsed JSON object (via `parse_json`) into the UDTF. The UDTF then converts the variant object back to a JSON string using `.toJson()` in Python and yields it. The test verifies that the resulting DataFrame contains correct JSON representation.

#### `test_udtf_with_variant_output`
This test verifies that a UDTF can emit Variant types. The UDTF creates `VariantVal` instances manually using byte sequences and yields them. In the SQL part of the test, the returned variants are cast back to JSON using `to_json`, and compared with expected JSON strings representing objects with varying character values.

#### `test_udtf_with_wrong_num_input`
This test verifies that proper exceptions are thrown when a UDTF is invoked with the wrong number of input arguments. It tests two error conditions: calling the function with too few arguments, and calling it with too many positional arguments, expecting errors defined in `BaseUDTFTestsMixin`.

#### `test_udtf_with_wrong_num_output`
This test checks for schema mismatch errors when the number of values yielded by a UDTF instance does not match the count of columns specified in its returnType schema. It verifies both cases: returning too few columns and returning too many columns, both raising a `PythonException` with the code "UDTF_RETURN_SCHEMA_MISMATCH".

#### `test_udtf_with_zero_arg_and_invalid_return_value`
This test validates the requirement that a UDTF's `eval` method must return an iterable object (like a list or yield generator). Here, `eval` returns a raw integer, and the test ensures this results in a `PythonException` with the text "UDTF_RETURN_NOT_ITERABLE".

#### `test_udtf_yield_multi_cols`
This test simply checks that a UDTF can correctly yield rows with multiple columns. A basic function is provided that accepts an integer and yields two columns based on it. The output structure is validated.

#### `test_udtf_yield_multi_row_col`
This test validates that a UDTF can yield multiple separate rows, where each row contains multiple columns. A function takes two parameters and yields three rows of calculations for them. The test compares the result with array literals.

#### `test_udtf_yield_multi_rows`
This test checks that a UDTF can produce multiple rows from processing a single input value. The `eval` method yields two separate tuples representing separate rows of a single column.

#### `test_udtf_yield_single_row_col`
This is a fundamental test checking that a UDTF can receive a single input and yield a single row with a single column. It's the most basic successful operation scenario.

### Class: `UDTFParityTests`

#### `test_array_output_type_casting`
Verifies that UDTFs returning arrays of various types are correctly cast to expected types or raise exceptions for incompatible types.

#### `test_df_asTable`
Verifies that passing a DataFrame as a table argument to a UDTF works correctly, comparing against equivalent SQL queries using TABLE.

#### `test_df_asTable_chaining_methods`
Verifies that chaining methods like partitionBy, orderBy, and withSinglePartition on asTable() arguments for UDTFs works correctly and enforces proper call order.

#### `test_docstring`
Verifies that UDTF classes preserve docstrings for the class itself, __init__, analyze, eval, and terminate methods when wrapped.

#### `test_eval_with_exception`
Verifies that exceptions raised in the eval method of a UDTF are correctly propagated as PythonException with error class UDTF_EXEC_ERROR.

#### `test_inconsistent_output_types`
Verifies behavior of UDTFs yielding rows with types inconsistent with the declared return type, checking if they are correctly handled or cast.

#### `test_init_with_exception`
Verifies that exceptions raised in the __init__ method of a UDTF are correctly propagated as PythonException with error class UDTF_EXEC_ERROR.

#### `test_map_output_type_casting`
Verifies that UDTFs returning maps of various types are correctly cast to expected types or raise exceptions for incompatible types.

#### `test_nondeterministic_udtf`
Verifies that non-deterministic UDTFs (e.g., using random) can be registered and called in both DataFrame API and SQL queries.

#### `test_numeric_output_type_casting`
Verifies that UDTFs returning numeric types (e.g., int) are correctly cast to expected types or raise exceptions for incompatible types like date/timestamp.

#### `test_numeric_string_output_type_casting`
Verifies that UDTFs returning numeric strings (e.g., "1") are correctly cast to expected types like binary, or result in None for incompatible numeric types.

#### `test_simple_udtf`
Verifies basic functionality of a simple UDTF yielding multiple columns.

#### `test_simple_udtf_with_analyze`
Verifies that UDTFs with static analyze methods work correctly, determining schema dynamically, and can be called via DataFrame API or SQL.

#### `test_string_output_type_casting`
Verifies that UDTFs returning string types (e.g., "hello") are correctly cast to expected types like binary, or result in None for incompatible numeric types.

#### `test_struct_output_type_casting_dict`
Verifies that UDTFs returning dictionaries are correctly cast to expected struct types with proper type conversion for fields.

#### `test_struct_output_type_casting_row`
Verifies struct output type casting for rows, expecting a specific exception (e.g., PickleException in this test case).

#### `test_terminate_with_exceptions`
Verifies that exceptions raised in the terminate method of a UDTF are correctly propagated as PythonException with error class UDTF_EXEC_ERROR.

#### `test_udtf_access_spark_session`
Verifies that UDTFs can access the active Spark session (calls super).

#### `test_udtf_analyze_with_logging`
Verifies that logging within the analyze method of a UDTF correctly records warning messages to the Python worker log table when logging is enabled.

#### `test_udtf_analyze_with_pyspark_logger`
Verifies that using PySparkLogger within the analyze method of a UDTF correctly records warning messages with custom context to the Python worker log table.

#### `test_udtf_binary_type`
Verifies that UDTFs handle binary types correctly, respecting the spark.sql.execution.pyspark.binaryAsBytes configuration to map to either bytes or bytearray.

#### `test_udtf_cleanup_with_exception_in_eval`
Verifies that the cleanup method of a UDTF is still called even if an exception is raised during the eval method.

#### `test_udtf_cleanup_with_exception_in_terminate`
Verifies that the cleanup method of a UDTF is still called even if an exception is raised during the terminate method.

#### `test_udtf_decorator`
Verifies that UDTFs can be created using the @udtf decorator with specified return types.

#### `test_udtf_determinism`
Verifies that UDTFs are marked as non-deterministic by default and can be explicitly marked as deterministic using asDeterministic().

#### `test_udtf_eval_returning_non_tuple`
Verifies that UDTFs yielding or returning non-tuple objects (like integers or single objects) raise a PythonException with error class UDTF_INVALID_OUTPUT_ROW_TYPE.

#### `test_udtf_eval_returning_tuple_with_struct_type`
Verifies that UDTFs can return tuples representing struct types, but raise UDTF_RETURN_SCHEMA_MISMATCH if they return flat values instead of tuples for struct fields.

#### `test_udtf_eval_returning_udt`
Verifies that UDTFs can yield rows containing User Defined Types (UDTs) specified in the struct schema.

#### `test_udtf_eval_taking_udt`
Verifies that UDTFs can accept rows containing User Defined Types (UDTs) as arguments and process them.

#### `test_udtf_eval_with_no_return`
Verifies that UDTFs with eval methods that don't return anything or have an empty return statement return an empty DataFrame.

#### `test_udtf_eval_with_return_stmt`
Verifies that UDTFs with eval methods returning a list of tuples work correctly, producing a row for each tuple.

#### `test_udtf_init`
Verifies basic UDTF functionality with an __init__ method that sets state used in eval.

#### `test_udtf_init_with_additional_args`
Verifies that UDTF classes with constructor arguments other than self raise an error when instantiated, as constructor arguments are not supported.

#### `test_udtf_kill_on_timeout`
Verifies that worker processes running a long UDTF operation are killed when idle timeout triggers if appropriate configuration is enabled.

#### `test_udtf_no_eval`
Verifies that attempting to register a UDTF without an eval method raises a PySparkAttributeError with error class INVALID_UDTF_NO_EVAL.

#### `test_udtf_nullable_check`
Verifies that UDTFs yielding rows with null values in non-nullable fields raise a PySparkRuntimeError, and succeed if fields are nullable.

#### `test_udtf_pickle_error`
Verifies that UDTFs that fail to serialize (pickle) raise a PySparkPicklingError with error class UDTF_SERIALIZATION_ERROR.

#### `test_udtf_register_error`
Verifies that attempting to register non-UDTF objects (like regular UDFs or plain classes without UDTF properties) raises a PySparkTypeError with error class CANNOT_REGISTER_UDTF.

#### `test_udtf_registration`
Verifies registration of UDTFs and execution via SQL queries, yielding multiple rows per input.

#### `test_udtf_segfault`
Verifies that enabling faulthandler for UDTFs correctly intercepts segmentation faults during either analyze or eval methods and reports them.

#### `test_udtf_terminate`
This test verifies the behavior of a User-Defined Table Function (UDTF) that implements the `terminate` method. The UDTF `TestUDTF` yields an "input" row for each evaluated integer, incrementing a counter and summing the values. In the `terminate` method, it yields the total count and the average of the evaluated values. The test first checks this behavior with a single literal value. Then, it registers the UDTF and uses it in a SQL query with a `LATERAL` join over a range of numbers (0 to 9, step 1, split into 2 partitions). It verifies that the `terminate` method is called for each partition and correctly outputs the count and average for the rows processed in that partition.

#### `test_udtf_terminate_with_additional_args`
This test ensures that an error is raised if the `terminate` method of a UDTF expects additional arguments that are not provided. The `TestUDTF` defines a `terminate` method that requires an argument `a`. The test attempts to use this UDTF by calling `.show()`, which triggers execution, and asserts that a `PythonException` is raised with a message indicating that the `terminate` method is missing the required positional argument 'a'.

#### `test_udtf_terminate_with_wrong_num_output`
This test verifies that PySpark raises a `UDTF_RETURN_SCHEMA_MISMATCH` exception when a UDTF's `terminate` method yields a number of columns that does not match the return type declared in the `@udtf` decorator. Two scenarios are tested: one where the `terminate` method yields more columns (3) than expected (2), and another where it yields fewer columns (1) than expected (2). Both cases are expected to raise a `PythonException` with the specific error class or message.

#### `test_udtf_with_analyze`
This test checks the functionality of the `analyze` static method in a UDTF. The `analyze` method is used to dynamically determine the output schema based on the input arguments. In this test, `analyze` takes an argument, asserts that it is a constant expression and not a table, and returns an `AnalyzeResult` with a schema containing a single field "a" of the same data type as the input. The test iterates through various input types (Integer, String, Array, Map, Struct) and verifies that the output schema and data match the expected results. It also tests this using a SQL query.

#### `test_udtf_with_analyze_arbitrary_number_arguments`
This test validates that a UDTF's `analyze` method can accept an arbitrary number of arguments using the `*args` syntax. The `analyze` method constructs an output schema by creating a `StructField` for each input argument, named "colN" where N is the argument index, using the data type of that argument. The `eval` method simply yields the arguments it receives. The test checks three scenarios: one integer argument, an integer and a string argument called via SQL, and no arguments. It verifies the schema and results in each case.

#### `test_udtf_with_analyze_decorator`
This test verifies the use of the `@udtf` decorator on a class that contains a static `analyze` method. The `analyze` method returns a fixed schema of two string columns ("c1" and "c2"). The `eval` method yields the tuple ("hello", "world"). The test confirms that the UDTF returns the expected row when called directly via the Python API and when invoked as a SQL function after registration.

#### `test_udtf_with_analyze_decorator_parens`
This test is nearly identical to `test_udtf_with_analyze_decorator` but checks that the `@udtf()` decorator with parentheses works just as well as the one without them. It defines a class with a static `analyze` method returning a fixed schema, and an `eval` method yielding a row. It then registers the function and verifies that both the DataFrame generated from calling it directly and the one from a SQL query yield the expected results.

#### `test_udtf_with_analyze_kwargs`
This test ensures that a UDTF's static `analyze` method can accept keyword arguments via `**kwargs`. The `analyze` method asserts that the input contains keys 'a' and 'b' with specific types and constant values. It then constructs a sorted output schema derived from the provided keyword arguments' types. The `eval` method also accepts `**kwargs` and returns the associated values sorted by keys. The test evaluates the function using both direct Python calls with named arguments and SQL queries with explicit parameter names (e.g., `a => 10`), testing different argument orderings to ensure behavior is correct.

#### `test_udtf_with_analyze_multiple_arguments`
This test verifies the correct behavior when a UDTF's static `analyze` method takes multiple explicit positional arguments. In this case, `analyze` receives two arguments and constructs an output schema mapping them to fields "a" and "b" with types matching the arguments. The `eval` method simply returns a tuple of the arguments. The test exercises this by calling the function both directly via Python (passing integer and string literals) and from a SQL query, and asserts that both the result data and schema align correctly.

#### `test_udtf_with_analyze_non_staticmethod`
This negative test checks that defining the `analyze` method as an instance method (instead of a static method) in a UDTF raises the appropriate errors. First, when registering a UDTF that has a non-static `analyze` method but lacks an explicit `returnType`, it asserts that a `PySparkAttributeError` is thrown with the error class `INVALID_UDTF_RETURN_TYPE`. Second, if a UDTF with a non-static `analyze` method is combined with an explicit `returnType` argument, a similar exception with the class `INVALID_UDTF_BOTH_RETURN_TYPE_AND_ANALYZE` should be raised, signaling that a class with `analyze` cannot also be assigned an overriding `returnType`.

#### `test_udtf_with_analyze_null_literal`
This test checks how a UDTF's static `analyze` method behaves when provided with a `null` literal (`lit(None)`). In the UDTF `TestUDTF`, the `analyze` method captures the `dataType` of the input and returns a schema containing a field "a" of that type. The test passes a `null` literal into this function, which evaluates to a schema with field type `NullType`. It verifies that the resultant DataFrame inherits this specific schema and evaluates to the correct row containing `None`.

#### `test_udtf_with_analyze_order_by_override_nulls_first`
This test examines the capacity of a UDTF's `analyze` method to influence the ordering of records within its partition, specifically examining null record placement. The static `analyze` method requests execution in a single partition (`withSinglePartition=True`) and applies an order based on the 'id' column. In doing so, it toggles the `overrideNullsFirst` flag to both `True` and `False`. The test verifies that setting this flag to `True` sorts a dataset of `[1, None]` such that `None` precedes `1`, while setting it to `False` results in `1` preceding `None`.

#### `test_udtf_with_analyze_raising_an_exception`
This test ensures that when a UDTF's `analyze` static method raises an unhandled Python exception, PySpark correctly wraps it in an `AnalysisException` containing the original error message. The `TestUDTF` implements an `analyze` method that intentionally fails with the error text "Failed to analyze.". The test attempts to collect the DataFrame resulting from invoking that UDTF and verifies that this specific error propagates as intended.

#### `test_udtf_with_analyze_table_argument`
This test verifies the ability of a UDTF's static `analyze` method to handle a table argument (`TABLE (...)`). The test's `analyze` static method asserts that the supplied `AnalyzeArgument` is indeed a table and not a constant expression. It generates an output schema derived from the schema of the provided table's first column (extracting its `dataType`). The test registers the UDTF and invokes it in SQL over the table literal generated from a range of values `[0, 8)`. It asserts both the correct resulting schema as well as checking that `eval` is invoked on each row correctly, emitting values strictly greater than 5.

#### `test_udtf_with_analyze_table_argument_adding_columns`
This test verifies that a UDTF's `analyze` method can read the schema of an input table argument and add a new column to it. The `analyze` method receives an argument representing a table, asserts that it is indeed a table, and constructs an output schema by adding a boolean column named "is_even" to the incoming table's schema. The `eval` method yields the value of the 'id' column and evaluates whether it is even. The test invokes this by executing a SQL command on a range of 4 numbers, expecting the results to match the original ID column accompanied by calculated parity boolean states.

#### `test_udtf_with_analyze_table_argument_repeating_rows`
This test evaluates a UDTF that repeats each row of a provided table argument a specified number of times, determined by an integer argument. The `analyze` method validates that the first argument is a scalar integer (between 1 and 10) and that the second argument is a table, throwing an exception if these conditions fail. It replicates the input table's schema. The `eval` function carries out the repetition. The test checks both successful invocations (passing `2` and `1 + 1` as repetition counters) as well as verifying that error messages correctly propagate when negative rules are breached (passing `0`, passing multiple tables, or not passing a table where requested).

#### `test_udtf_with_analyze_table_select`
This test examines the UDTF `analyze` functionality when using the `select` option in its `AnalyzeResult`. The `analyze` method specifies that only the column named 'id' should be passed into the UDTF by supplying `select=[SelectedColumn("id")]`. Consequently, the test verifies that even though the provided table DataFrame possesses both columns `id` and `value`, the `eval` function solely receives instances of the 'id' field, and no others. It asserts the correct reduced result on a generated test dataset of 3 elements.

#### `test_udtf_with_analyze_taking_keyword_arguments`
This test confirms that an `analyze` method can be defined to handle standard catch-all Python keyword arguments using `**kwargs`. The test defines a constant schema mapping of fields "a" and "b" as strings in `analyze`, and an `eval` function yielding "hello" and "world". The test successfully invokes instances with zero args and with named args (`a=>1`). It then applies negative testing by checking that if regular, non-named positional arguments are passed where they are not expected, an `AnalysisException` with the specific text `tooManyPositionalArguments` correctly returns.

#### `test_udtf_with_analyze_taking_wrong_number_of_arguments`
This test enforces that when a registered UDTF with an explicit number of positional arguments in its `analyze` signature is called with an incorrect count, standard analysis exceptions result. Here `TestUDTF.analyze` takes exactly two arguments ('a' and 'b'). The test attempts calls providing only one argument as well as three arguments, verifying that in both instances, calls fail with a raised `AnalysisException` mentioning arguments.

#### `test_udtf_with_analyze_using_accumulator`
This test calls the superclass implementation of `test_udtf_with_analyze_using_accumulator`. Based on the name, it likely verifies that a UDTF can access and use Spark accumulators during its `analyze` phase or execution.

#### `test_udtf_with_analyze_using_archive`
This test calls the superclass implementation of `check_udtf_with_analyze_using_archive` with the path ".". It likely verifies that a UDTF can access files or resources distributed via Spark's `archive` functionality during its `analyze` phase or execution.

#### `test_udtf_with_analyze_using_broadcast`
This test calls the superclass implementation of `test_udtf_with_analyze_using_broadcast`. It likely verifies that a UDTF can access broadcast variables during its `analyze` phase or execution.

#### `test_udtf_with_analyze_using_file`
This test calls the superclass implementation of `check_udtf_with_analyze_using_file` with the path ".". It likely verifies that a UDTF can access files distributed via Spark's `addFile` functionality during its `analyze` phase or execution.

#### `test_udtf_with_analyze_using_pyfile`
This test checks that a UDTF can successfully resolve and use Python files added dynamically via Spark's `addPyFile` interface in its `analyze`, `eval`, and `terminate` methods. It creates a temporary directory with a module, adds that path via the spark context, and defines a UDTF that calls a dummy lambda inside the imported script. The test confirms that this call operates identically across Python direct calls and evaluation via SQL strings.

#### `test_udtf_with_analyze_using_zipped_package`
This test is highly identical to `test_udtf_with_analyze_using_pyfile`, but tests that imported logic distributed inside a *zipped* package format can also be reliably triggered and accessed inside UDTF steps. It manually creates a temporary directory structure, creates a standard `__init__.py` file in a sub-folder, zips that directory, and applies that resource through Spark commands before running the DataFrame collection checks.

#### `test_udtf_with_array_input_type`
This test verifies the processing behavior when passing an array literal argument to a UDTF. The UDTF just prints the string format of its arguments. The assertion checks that the return array correctly registers as mapped integers inside brackets; this accommodates a possible type mapping deviation where numbers can map as either standard Python `ints` or specific `np.int32` numbers.

#### `test_udtf_with_array_output_types`
This test verifies a UDTF that produces an array output. The `TestUDTF` accepts an integer and outputs an array of integers `[x, x + 1, x + 2]`. The result of checking this with the literal value `1` is verified against the array `[1, 2, 3]`.

#### `test_udtf_with_both_return_type_and_analyze`
This test is very similar to part of `test_udtf_with_analyze_non_staticmethod`. It directly tests that calling the `@udtf` constructor with *both* a class that has an `analyze` method and a supplied custom `returnType` fails actively. It expects that the call throws a `PySparkAttributeError` with error class `INVALID_UDTF_BOTH_RETURN_TYPE_AND_ANALYZE`.

#### `test_udtf_with_collated_string_types`
This test checks that a UDTF correctly processes and maps custom string collation types. It sets up a frame with fields using four different collations (the default, `UTF8_BINARY`, `UTF8_LCASE`, and `UNICODE`) and defines a UDTF that appends fixed characters onto these values and maintains those specific collation string types on the output return spec. The test applies this via lateral join and tests that both strings and column schema field types resolve to those specific instances properly.

#### `test_udtf_with_conditional_return`
This test verifies a UDTF that optionally yields values based on a conditional statement. The `eval` method yields the provided integer only if it is strictly greater than 5. It registers the function and uses a SQL statement with a `LATERAL` join over a range of numbers `[0, 8)`. It asserts that the result frame has size 2 and corresponds only to ranges `6` and `7`.

#### `test_udtf_with_conditional_return_dataframe`
This test repeats the check performed by `test_udtf_with_conditional_return`, but compares DataFrame API use against the SQL statement execution format directly. It calls `TestUDTF` via the Python `DataFrame.lateralJoin` API over a range of size 8 and asserts that the resulting row set matches perfectly against the row set created by invoking the same logic as custom pure SQL text strings.

#### `test_udtf_with_empty_output_schema_and_non_empty_output`
This negative test checks that applying an empty output schema using `@udtf(returnType=StructType())` fails if rows are actually returned during processing. The function tries to return the tuple `(1,)` while claiming to output nothing. It expects a `PythonException` with message `UDTF_RETURN_SCHEMA_MISMATCH`.

#### `test_udtf_with_empty_output_types`
This test verifies the execution when a UDTF with an explicitly empty output schema (`StructType()`) actually yields empty tuples (`tuple()`). In this case, the execution is valid and is expected to evaluate to a DataFrame holding one empty `Row`.

#### `test_udtf_with_empty_yield`
This test evaluates the behavior of a UDTF when the `eval` method yields an empty `yield` statement without returning any columns, despite declaring that it returns an integer. It shows that in this instance, PySpark generates a column populated with `None` where values were requested, returning a collection containing `Row(a=None)`.

#### `test_udtf_with_int_and_table_argument_identifier`
This test checks that a UDTF can consume values by taking both a standard primitive integer and a Table object identified by a named view reference in string form. It registers a UDTF that filters the passed table's records where their 'id' field is greater than requested parameter `i`. The test creates a dummy temporary view 'v' filled with values `[0, 8)` and confirms that running the SQL command over parameter string `(5, TABLE (v))` correctly filters the dataset to return just `[6, 7]`.

#### `test_udtf_with_int_and_table_argument_query`
This test exercises exactly the same scenario as `test_udtf_with_int_and_table_argument_identifier` but uses direct embedded SQL subqueries in place of the previously named view identifier in the execution query. The `TABLE (SELECT ...)` syntax is provided directly inline and the expected output rows are matched correctly.

#### `test_udtf_with_invalid_return_type`
This test evaluates what happens when setting an invalid return type on the `@udtf` decorator, such as assigning it a simple type specifier text like `"int"` instead of a proper `StructType` format. In Apache Spark UDTFs, the output must be mapped onto a schema collection container. The test asserts that compiling a UDTF configured with this incorrect parameter throws an `InvalidPlanInput` exception referencing an invalid type.

#### `test_udtf_with_invalid_return_value`
This test ensures that if a UDTF's `eval` function attempts to directly return a plain scalar non-iterable value instead of yielding or returning an iterable, Spark flags this issue. Here, `TestUDTF` yields its evaluation simply with `return a`. The assertion confirms that the command execution results in a `PythonException` with message `UDTF_RETURN_NOT_ITERABLE`.

#### `test_udtf_with_invalid_return_value_in_terminate`
This test functions as an add-on to `test_udtf_with_invalid_return_value`. It evaluates what happens when a scalar is returned instead of an iterable in the `terminate` method instead of in `eval`. In this case, `terminate` returns `1`. The test checks that execution accurately crashes with `UDTF_RETURN_NOT_ITERABLE`.

#### `test_udtf_with_kwargs`
This test examines the parsing of named keyword arguments through `**kwargs` inside the UDTF's `eval` function. The test invokes the function using different combinations of named variables `a` and `b` in Python expressions and SQL queries. It ensures that the result data resolves to the requested row elements despite changing assignment ordering. The test further verifies errors are flagged when providing duplicate assignments in a query or when mixing positional and keyword items incorrectly in the SQL text format.

#### `test_udtf_with_lateral_join`
This test validates that standard `LATERAL` joins function properly in SQL when linked against custom registered UDTFs. It instantiates a function that generates a sum and difference row for each pair of provided values. It evaluates on standard frame `[0, 1]` and `[1, 2]`, verifying that the table join produces four specific resulting rows.

#### `test_udtf_with_lateral_join_dataframe`
This test performs an in-depth analysis of the `lateralJoin` DataFrame API as applied over UDTFs. It tests three variations of invoking keyword parameters alongside normal execution, ensuring all match perfect correspondence to SQL command strings. Then, it expands by testing zero-arg functions applied over an existing frame, and finally, testing parameter-bound evaluations mapping against specific record items via `outer()` calls.

#### `test_udtf_with_logging`
This test ensures that log statements issued within a UDTF worker successfully record and transfer to Spark's internal log collections. It defines a function that prints a warning log message on each integer processed, applies that against dataset `[5, 10]` after setting the `"spark.sql.pyspark.worker.logging.enabled"` configuration parameter to true, and checks whether the recorded results from calling system view `python_worker_logs` precisely equal the text issued within the UDTF body.

#### `test_udtf_with_map_input_type`
This test verifies the ability to provide a mapped dictionary literal into a UDTF's arguments list. The registered test evaluates the text value on row data, applying command over SQL structure `map('key', 'value')`, and checks that the returned value yields dictionary bracket strings `"{'key': 'value'}"`.

#### `test_udtf_with_map_output_types`
This test evaluates the generation of typed map results inside a UDTF. The return type specifies field schema `x: map<int, string>`. The code provides dictionary keys as integers and string values inside execution records. Running direct call on literal `1` provides dictionary map frame mapping value `1` to string `"1"`.

#### `test_udtf_with_named_arguments`
This test ensures that calling a UDTF with named arguments resolves parameters to correct instances regardless of parameter placement in code. The UDTF only requires mapping parameter `a` and rejects or ignores field `b` without performing any operations. The test calls both SQL and Python frame APIs under different keyword assignments, checking that `10` is consistently returned.

#### `test_udtf_with_named_arguments_and_defaults`
This test exercises a UDTF where both the `analyze` method and the `eval` method use default optional arguments. In the static `analyze` function, it handles receiving an optional second argument 'b', asserting specific values and types, and returning a differing resolved output schema accordingly (providing an integer field if missing, or a string field if supplied). The `eval` method defaults parameter `b` to `100`. The test validates invocations both without specifying `b` (expecting results including `100`) and specifying `b` (as string `"z"`). It exercises diverse API pathways (Python calls, SQL strings).

#### `test_udtf_with_named_arguments_lateral_join`
This test combines SQL `LATERAL` joins with explicit named parameter mappings. The query joins a test values structure holding strings and numbers against a function whose `eval` yields the first element. The test attempts two diverse calls in SQL that swap assignment orderings of named arguments (`a => a, b => b` vs `b => b, a => a`) and confirms that in both runs, values `[0, 1]` resolve correctly without falling victim to ordering mishaps.

#### `test_udtf_with_named_arguments_negative`
This negative test checks the rules of named keyword mappings in UDTF SQL statements, identifying several failure conditions. It asserts that passing two values for a single named argument throws `DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT`. It checks that following a named argument with a positional argument triggers `UNEXPECTED_POSITIONAL_ARGUMENT`. It checks that failing to supply required arguments results in `missingARequiredArgument`. Finally, it confirms that passing argument mapping as both positional and named simultaneously yields `multipleValuesForArgument`.

#### `test_udtf_with_named_table_arguments`
This test verifies the parsing of table arguments named as keywords inside a UDTF. The defined function maps its keyword reference `a` onto a provided source table and emits values pulled from it. Four diverse executions are tested across varying orders inside direct API calls as well as inside custom SQL statements that apply `a => TABLE(...)`. All are checked against matching record responses.

#### `test_udtf_with_neither_return_type_nor_analyze`
This test verifies that defining a User-Defined Table Function (UDTF) without specifying a return type or implementing an `analyze` method results in a `PySparkAttributeError` with the error class `INVALID_UDTF_RETURN_TYPE`. The test creates a simple UDTF class with an `eval` method yielding two strings, attempts to register it using the `udtf` decorator without return type arguments, and asserts that the expected error is raised.

#### `test_udtf_with_nested_variant_input`
This test verifies that UDTFs can accept nested variant types as input. It covers three cases: a struct containing a variant, an array of variants, and a map with variant values. For each case, it defines a UDTF that extracts the variant value, converts it to a JSON string using `.toJson()`, and yields it 10 times. It registers the UDTFs, calls them with appropriate SQL queries constructing the nested variant structures, and asserts that the output DataFrame matches the expected rows.

#### `test_udtf_with_nested_variant_output`
This test verifies that UDTFs can produce nested variant types as output. Similar to the input test, it covers three cases: a struct containing a variant, an array of variants, and a map with variant values. In each case, it constructs a `VariantVal` directly using byte representations (due to lack of public API at the time) and yields it. It registers the UDTFs, runs SQL queries that select the output and convert the variants to JSON, and asserts that the results match the expected JSON strings derived from the byte inputs.

#### `test_udtf_with_no_handler_class`
This test ensures that trying to create a UDTF from something other than a class (like a function or an integer) raises a `PySparkTypeError` with the error class `INVALID_UDTF_HANDLER_TYPE`. It tests both decorating a function directly and passing an integer to the `udtf` constructor, verifying the specific error message parameters in each case.

#### `test_udtf_with_non_empty_output_schema_and_empty_output`
This test checks that a runtime error is thrown if a UDTF yields an empty tuple when its return schema specifies a non-empty output (specifically, a single integer column). The test asserts that a `PythonException` containing the message `UDTF_RETURN_SCHEMA_MISMATCH` is raised during DataFrame collection.

#### `test_udtf_with_nondeterministic_input`
This test verifies that a UDTF can be called with a non-deterministic expression (like `rand() * 100`) as an argument. The UDTF evaluates if the argument is greater than 100, which should never be the case here since `rand()` yields values between 0 and 1. The test asserts that the result matches the expected output of `Row(x=0)`.

#### `test_udtf_with_none_input`
This test verifies that passing a null value (None in Python, NULL in SQL) to a UDTF works correctly and produces a row with a null value. It tests this behavior using both the DataFrame API (`TestUDTF(lit(None))`) and a SQL query after registering the UDTF.

#### `test_udtf_with_none_output`
This test confirms that a UDTF can yield rows containing null values. The defined UDTF yields its input integer and then yields a null value. The test verifies this by checking the direct output, and then exercises joins (both inner and left) with another DataFrame to ensure the null value behaves as expected in standard DataFrame operations.

#### `test_udtf_with_pandas_input_type`
This test documents the behavior when a UDTF expects Pandas Series as arguments but receives regular scalar types (integers) in a lateral join. Because the expected Pandas support might not be fully functional or invoked correctly here, the `eval` method tries to call `.corr()` on an integer, leading to an `AttributeError`. The test captures this failure as a `PythonException`.

#### `test_udtf_with_prepare_string_from_analyze`
This test demonstrates a more complex UDTF that uses a custom `AnalyzeResult` (subclassed to include a string buffer). The `analyze` method validates its arguments, ensuring the first is a non-empty string and not a table, and uses the first argument's value to populate the custom `buffer` property in its return object. The UDTF's `__init__` method receives this custom result object and extracts the buffer, which is finally yielded in the `terminate` method along with a count of rows processed in the `eval` method. This exercises the ability to pass metadata computed during query planning (`analyze`) down to the runtime instances of the UDTF.

#### `test_udtf_with_scalar_analyze_returning_wrong_result`
This test verifies that PySpark throws an `AnalysisException` with specific error messages if a UDTF's `analyze` method returns invalid results for a scalar UDTF (one that doesn't take a table argument). It tests several bad return values, including returning a raw data type instead of an `AnalyzeResult`, passing a non-struct schema into `AnalyzeResult`, and setting properties like `withSinglePartition` and `partitionBy` that are only valid when the function operates on a table argument.

#### `test_udtf_with_skip_rest_of_input_table_exception`
This test checks that throwing the `SkipRestOfInputTableException` inside the `eval` method successfully terminates row processing for the current partition. It tests two scenarios: 1) One big partition where processing halts after the 4th row, producing a single final summary row, and 2) Multiple partitions formed by `PARTITION BY floor(id / 10)` where each partition is processed independently and the exception successfully cuts processing short on a per-partition basis.

#### `test_udtf_with_struct_input_type`
This test verifies that a UDTF can take a struct as a parameter and access its fields. The test UDTF receives a struct representing a person, extracts the name and age fields using object attribute syntax (`person.name`, `person.age`), and yields a combined string. The test calls it with a query using `named_struct` and checks for the correctly formatted result.

#### `test_udtf_with_struct_output_types`
This test ensures that a UDTF can produce complex output types like a struct. The UDTF takes an integer and yields a tuple containing a single dictionary representing the struct with keys `a` and `b`. The result is verified against a list containing a Row where field `x` is itself another Row.

#### `test_udtf_with_table_analyze_returning_wrong_result`
Similar to the scalar analyze test, this test verifies that specific errors are thrown when processing a UDTF with table arguments whose `analyze` method returns incorrect objects. It covers cases like passing an array of `OrderingColumn` instead of `PartitioningColumn` inside `partitionBy`, passing `PartitioningColumn` inside `orderBy`, or setting custom properties on properties where they aren't supported. The test expects an `AnalysisException` matching the specified regex patterns.

#### `test_udtf_with_table_argument_and_analyze_kwargs`
This test exercises UDTFs that take both table arguments and scalar arguments using keyword syntax, specifically focusing on how these arguments are passed to the `analyze` method. The `analyze` method expects specific kwargs, makes assertions about their types and values (e.g., distinguishing table arguments from scalar arguments), and dynamically constructs the output schema. The test checks this by running the UDTF both with SQL and DataFrame API in different keyword arrangements and asserts the correct data structure and value propagation.

#### `test_udtf_with_table_argument_and_kwargs`
This test verifies that UDTFs can accept both table arguments and scalar arguments using keyword syntax in the `eval` method without defining an explicit `analyze` method. The test UDTF's `eval` method directly unpacks `**kwargs`, accessing the single 'id' column from the table argument and scalar string 'b'. It exercises both SQL and DataFrame API calls with varying keyword orders and checks for matching result sets.

#### `test_udtf_with_table_argument_and_partition_by`
This comprehensive test verifies the behavior of the `PARTITION BY` clause when applied to a table argument in a UDTF call. It uses a custom UDTF that tracks state per partition by asserting that every input row it sees in a single instantiation has the exact same partition key. It accumulates values per partition in `eval` and returns aggregated summaries for that partition in `terminate`. The test exercises several scenarios including standard column partitions, constant partitions, and combining `PARTITION BY` with `LATERAL` joins.

#### `test_udtf_with_table_argument_and_partition_by_and_order_by`
This test builds on the partition test by adding the `ORDER BY` clause within partitions. The UDTF observes rows within a partition and in its `terminate` method, it returns the *last* input value seen. By switching the ordering between ascending and descending (e.g., `input ASC` vs `input DESC`), the value considered 'last' in the partition flips between 2 and 1. The test correctly captures this flipping behavior, ensuring that Spark provides rows in the requested sort order within partitions to the UDTF instances.

#### `test_udtf_with_table_argument_and_partition_by_no_terminate`
This test confirms that a UDTF without a `terminate` method still works properly with a `TABLE` argument that has a `PARTITION BY` clause. It instantiates the UDTF, processes the rows, and simply returns whatever output was generated strictly within the `eval` cycles, without the final aggregation step that normally occurs in `terminate`.

#### `test_udtf_with_table_argument_cte_inside`
This test validates that standard table expressions (CTEs) defined *inside* the `TABLE()` argument passed to a UDTF function are processed correctly by Spark's parser and execution engine. The inner CTE builds a small range, and the UDTF effectively consumes and filters this table as described in its `eval` rules.

#### `test_udtf_with_table_argument_cte_outside`
Similar to the previous test, this verifies that UDTF table arguments correctly resolve external Common Table Expressions (CTEs) that are defined *outside* the function call itself, in the main query. It tests both a selective query on the CTE `TABLE (SELECT id FROM t)` and directly referencing the CTE `TABLE (t)`.

#### `test_udtf_with_table_argument_identifier`
This test verifies that a UDTF can take a temporary view directly as its `TABLE()` argument using the view identifier. It creates a temporary view `v` with a range of numbers and passes `TABLE (v)` to the UDTF, asserting that it produces correct results.

#### `test_udtf_with_table_argument_lateral_join`
This test checks that Spark correctly handles a UDTF combined with a lateral join when that UDTF is processing a table argument derived from the current row context. It constructs a simple sequence using `range`, does a lateral join against the UDTF, and yields rows passing through the standard execution path.

#### `test_udtf_with_table_argument_malformed_query`
This test expects that providing a malformed query or referencing a non-existent table in a UDTF `TABLE` argument produces an appropriate `AnalysisException` containing the error text `TABLE_OR_VIEW_NOT_FOUND`.

#### `test_udtf_with_table_argument_multiple`
This test demonstrates that passing multiple independent `TABLE` arguments to a UDTF behaves in a Cartesian product fashion at the row level. It tests the enforcement of the configuration `spark.sql.tvf.allowMultipleTableArguments.enabled`, where it expects a specific error `TABLE_VALUED_FUNCTION_TOO_MANY_TABLE_ARGUMENTS` when disabled, and works normally yielding all combinations when enabled.

#### `test_udtf_with_table_argument_query`
This is a basic test verifying that passing an inline select query as a `TABLE` argument to a UDTF works correctly, yielding expected rows after processing inside the test UDTF class.

#### `test_udtf_with_table_argument_unknown_identifier`
Similar to the malformed query test, this ensures that an `AnalysisException` with the message `TABLE_OR_VIEW_NOT_FOUND` is thrown if the user references an undefined or unknown identifier in the UDTF call `TABLE (v)`.

#### `test_udtf_with_table_argument_with_partition_by_and_order_by_from_analyze`
This test verifies that partitioning and ordering behavior can be defined directly by returning array structures in the UDTF's `analyze` method, rather than having to set them explicitly on the user-level SQL invocation via `TABLE(...) PARTITION BY ... ORDER BY ...`. The UDTF's `analyze` returns an `AnalyzeResult` specifying `partitionBy` and `orderBy` rules. The test fills in complex data, runs the UDTF, asserts that constraints were honored in `eval` (same partition per instance, strictly ascending order in `eval` calls), and checks for matching result summaries per partition.

#### `test_udtf_with_table_argument_with_single_partition`
This test confirms the functionality of combining `WITH SINGLE PARTITION` and `ORDER BY` directly in the query on a UDTF table argument. This directs Spark to push all rows from all generated ranges into a single partition handler in specific order. The test UDTF computes total counts, total sums, and captures the absolute last value seen, yielding a single summary row for the whole table.

#### `test_udtf_with_table_argument_with_single_partition_from_analyze`
This test is similar to the one above, but instead of specifying the processing rules in SQL via `WITH SINGLE PARTITION` and `ORDER BY`, it instructs the UDTF to direct its `analyze` method to return an `AnalyzeResult` setting `withSinglePartition=True` and defined `orderBy` rules. This fulfills the single partition execution mode dynamically.

#### `test_udtf_with_variant_input`
This test verifies that a UDTF can accept a raw variant type directly as an argument. The UDTF receives the variant, converts it to JSON using `.toJson()`, and yields it 10 times. The test calls it with `parse_json` in SQL and verifies the output.

#### `test_udtf_with_variant_output`
This test ensures that a UDTF can yield a variant type. Due to the lack of an easy constructor for Variants in Python at the time, it uses direct binary initialization on `VariantVal`. The test query consumes the UDTF and applies `to_json` to turn the variant structures back into JSON strings for clean match assertions against expected rows.

#### `test_udtf_with_wrong_num_input`
This test checks that errors are properly returned if a user passes too many or too few scalar arguments to a UDTF execution call. It instantiates a UDTF expecting one integer but calls it both with no arguments and with two arguments, asserting that expected text matches derived from `BaseUDTFTestsMixin` appear within the resulting `PythonException` stacks.

#### `test_udtf_with_wrong_num_output`
This test checks that a runtime error containing `UDTF_RETURN_SCHEMA_MISMATCH` is correctly raised if the number of values yielded by the UDTF does not match the number of columns defined in the return schema (both too few and too many columns cases are handled).

#### `test_udtf_with_zero_arg_and_invalid_return_value`
This test confirms that returning a single integer from the `eval` method of a zero-argument UDTF, rather than returning an iterable like a generator or list, raises a `PythonException` with the specific message `UDTF_RETURN_NOT_ITERABLE`.

#### `test_udtf_yield_multi_cols`
This simple test verifies that a UDTF can correctly return multiple columns in a single row by yielding a tuple (or multiple values) in the `eval` method. The test checks this by registering a UDTF returning two integers and asserting the result of running it against a literal input.

#### `test_udtf_yield_multi_row_col`
This test extends the previous one by yielding multiple rows, each with multiple columns, from a single execution of the UDTF's `eval` method. The UDTF computes three different values from two input integers and yields a row for each computation.

#### `test_udtf_yield_multi_rows`
This test verifies that a UDTF can yield multiple rows from a single call to `eval` for a single-column output. The UDTF yields the input and then the input incremented by one.

#### `test_udtf_yield_single_row_col`
This is the most basic UDTF test, verifying that yielding a single row with a single column from the `eval` method works correctly.

## File: `python/pyspark/sql/tests/connect/test_parity_unified_udf.py`

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

### Class: `UnifiedUDFParityTests`

#### `test_0_args`
Tests user-defined functions (UDFs) that take no arguments, comparing execution with and without Arrow optimization. It evaluates both implicit and explicit long return types and checks that the function evaluation type remains consistent with batch execution.

#### `test_arrow_optimized_python_udf`
Verifies behavior of Arrow-optimized Python UDFs across various signature styles (explicitly requesting Arrow, using type hints, and utilizing pandas and pyarrow specific types). It validates that the execution engine uses Arrow batching as intended.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_grouped_agg_arrow_iter_udf`
Tests grouped aggregate UDFs using an Arrow iterator as input. The function calculates a sum over batches of input arrays in an iterative fashion, validating results in both programmatically built DataFrames and SQL queries.

#### `test_grouped_agg_arrow_udf`
Tests grouped aggregate UDFs utilizing Arrow arrays directly instead of iterators. A max computation is used, evaluating coverage across DataFrame API and direct SQL queries.

#### `test_grouped_agg_pandas_udf`
Validates grouped aggregate UDF behavior when utilizing pandas Series in place of Arrow arrays. Max calculation is used, with testing coverage for both DataFrame and SQL execution paths.

#### `test_regular_python_udf`
Extensively tests non-Arrow regular Python UDFs when fallback triggers are tested or Arrow is explicitly disabled. Evaluates inferred versus explicit return types and confirms standard batch evaluation paths.

#### `test_scalar_arrow_iter_udf`
Tests scalar UDFs accepting Arrow array iterators as input, performing column operations like adding constants. Results are checked in both SQL and DataFrame operations.

#### `test_scalar_arrow_iter_udf_II`
Evaluates complex scalar UDF cases utilizing Arrow iterators that yield tuples of multiple columns for processing. The function performs addition on the grouped inputs and tests coverage in DataFrames and SQL.

#### `test_scalar_arrow_udf`
Tests scalar UDFs using Arrow arrays directly (non-iterator). The function adds a constant to an entire array of values and runs test verification against SQL and DataFrame paths.

#### `test_scalar_arrow_udf_II`
Evaluates complex scalar Arrow array UDFs that take multiple input columns for processing. Verifies addition across input columns in both standard DataFrame and direct SQL paths.

#### `test_scalar_pandas_iter_udf`
Tests scalar UDF cases passing pandas Series iterators as arguments to add a constant value. Evaluates behavior coverage across both standard programmatic and SQL paths.

#### `test_scalar_pandas_iter_udf_II`
Validates pandas iterator scalar UDFs receiving tuples representing multiple input columns. An elementwise add is performed, with coverage verification on DataFrames and SQL queries.

#### `test_scalar_pandas_udf`
Validates behavior for standard scalar UDF operations using basic elementwise operations over a single pandas Series argument. Both SQL and typical programmatic DataFrame paths are evaluated.

#### `test_scalar_pandas_udf_II`
Tests complex pandas UDF scenarios mapping multiple DataFrame columns to equivalent Series inputs for processing. Covers elementwise addition across columns in both SQL and DataFrame cases.

#### `test_window_agg_arrow_udf`
Tests Arrow-optimized grouped aggregate UDF functionality applied over Spark windows. Verifies computation equivalence against standard non-UDF DataFrame aggregates and test coverage on direct SQL paths.

#### `test_window_agg_pandas_udf`
Verifies pandas grouped aggregate UDF functionality when evaluated over a windowing operation. Confirms equivalence to baseline native DataFrame window max operations and checks both code and direct SQL usage.

## File: `python/pyspark/sql/tests/connect/test_resources.py`

### Class: `ResourceProfileTests`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_map_in_arrow_with_profile`
Evaluates `mapInArrow` function interactions when execution expects a specific, non-default requested hardware resource allocation profile. Task Context is inspected to confirm dynamic CPU counts match the given resource request.

#### `test_map_in_arrow_without_profile`
Tests typical baseline execution for `mapInArrow` without applying external hardware resource requests. Checks task context to ensure correct allocation under default resource pools.

#### `test_map_in_pandas_with_profile`
Similar to the map-in-arrow profile test, this evaluates Task Context CPU count assertions but handles batch operations utilizing the typical pandas DataFrame conversion interface.

#### `test_map_in_pandas_without_profile`
Performs normal baseline execution for `mapInPandas` without applying custom hardware requests. Checks resulting task context behavior under normal execution circumstances.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `python/pyspark/sql/tests/connect/test_session.py`

### Class: `SparkSessionTestCase`

#### `test_active_session_expires_when_client_closes`
Tests lifecycle management for active sessions in Connect, verifying that active handles correctly expire to None instead of persisting stale, closed references when low-level client connections terminate.

#### `test_creates_session_with_channel_builder`
Verifies support for initializing new active sessions via user-supplied custom gRPC channel builder configurations instead of simple string-based connection endpoints.

#### `test_creates_session_with_remote`
Checks typical session initialization support when targeting specific endpoints via plain connection strings instead of full-blown custom builder abstractions.

#### `test_default_session_expires_when_client_closes`
Verifies lifecycle teardown paths specifically for default session handles. Closes low-level clients to confirm stale instances properly purge from cache pools to prevent reference leaks.

#### `test_fails_to_create_session_without_remote_and_channel_builder`
Tests negative creation cases by throwing deliberate value errors whenever a new session builder request lacks both connection strings and builder alternatives.

#### `test_fails_to_create_when_both_remote_and_channel_builder_are_specified`
Tests session creation guard conditions preventing mutual conflict parameters. Verifies deliberate failures when both explicit strings and custom builder channels are provided simultaneously.

#### `test_session_create_sets_active_session`
Tests that explicitly requesting builder creation dynamically sets that resulting instance as the globally cached active session accessible through standard pools.

#### `test_session_stop`
Tests state changes triggered through standard session stop calls, ensuring session handles correctly track and update their internal 'is_stopped' boolean flag properties.

## File: `python/pyspark/sql/tests/connect/test_utils.py`

### Class: `ConnectUtilsTests`

#### `test_assert_approx_equal_arraytype_float`
Validates approximate DataFrame equality for ArrayType elements containing float data. Checks pass logic both when strict row ordering is required and when row order differences are permitted.

#### `test_assert_approx_equal_arraytype_float_custom_rtol_pass`
Tests custom tolerance overrides when validating approximate equality over Float Array arrays. Demonstrates passing behavior by widening acceptable error thresholds slightly beyond default limits.

#### `test_assert_approx_equal_arraytype_float_default_rtol_fail`
Validates deliberate rejection behavior when difference thresholds violate hardcoded default tolerance limits during float Array equality comparisons. Checks raised error codes directly.

#### `test_assert_approx_equal_decimaltype_custom_rtol_pass`
Tests approximate equality assertions targeting standard Decimal types after explicit type casts, demonstrating passing behavior when supplying custom float-like tolerance specifications.

#### `test_assert_approx_equal_doubletype_custom_rtol_pass`
Evaluates tolerance behavior controls by widening standard acceptance windows on direct Double columns containing minor differences. Asserts passing behavior under the wider spec.

#### `test_assert_approx_equal_fail_exact_pandas_df`
Tests negative exact match comparisons on pandas DataFrames. Forces failure assertions when requiring zero tolerance against data containing differences and inspects returned diagnostic error fields.

#### `test_assert_approx_equal_maptype_double`
Verifies that approximate equality checks cleanly handle complex Map representations where map values store float or double precision contents, testing under both ordered and unordered row settings.

#### `test_assert_approx_equal_nested_struct_double`
Verifies approximate comparison operations over complex nested Struct fields containing double precision floating values, under both ordered and unordered row constraints.

#### `test_assert_approx_equal_pandas_df`
Evaluates tolerance behavior directly against pure pandas DataFrames. Checks passing operations for float data containing small errors that fall beneath default thresholds.

#### `test_assert_data_frame_equal_not_support_streaming`
Asserts operations correctly throw errors if attempting to perform DataFrame comparison tests utilizing streaming structures instead of static batches. Validates that the raised error yields 'UNSUPPORTED_OPERATION'.

#### `test_assert_equal_approx_pandas_on_spark_df`
*No description available.*

#### `test_assert_equal_arraytype`
*No description available.*

#### `test_assert_equal_duplicate_col`
*No description available.*

#### `test_assert_equal_exact_pandas_df`
This test verifies that `assertDataFrameEqual` correctly identifies two identical Pandas DataFrames as equal, both when ignoring and respecting row order.

#### `test_assert_equal_exact_pandas_on_spark_df`
This test verifies that `assertDataFrameEqual` correctly identifies two identical Pandas-on-Spark DataFrames as equal, both when ignoring and respecting row order.

#### `test_assert_equal_exact_pandas_on_spark_df_no_order`
This test verifies that `assertDataFrameEqual` correctly identifies two Pandas-on-Spark DataFrames with the same elements but in different order as equal, by default ignoring row order.

#### `test_assert_equal_inttype`
This test verifies that `assertDataFrameEqual` correctly identifies two identical Spark DataFrames containing string and integer types as equal, both when ignoring and respecting row order.

#### `test_assert_equal_maptype`
This test verifies that `assertDataFrameEqual` correctly identifies two identical Spark DataFrames containing complex types like MapType as equal, both when ignoring and respecting row order.

#### `test_assert_equal_nested_struct_str`
This test verifies that `assertDataFrameEqual` correctly identifies two identical Spark DataFrames with complex nested structures containing string fields as equal, both when ignoring and respecting row order.

#### `test_assert_equal_nested_struct_str_duplicate`
This test verifies that `assertDataFrameEqual` correctly identifies two identical Spark DataFrames as equal when they contain nested structures with duplicate field names, both when ignoring and respecting row order.

#### `test_assert_equal_nulldf`
This test verifies that `assertDataFrameEqual` correctly handles case where both DataFrames are `None`, treating them as equal regardless of row order setting.

#### `test_assert_equal_nullrow`
This test verifies that `assertDataFrameEqual` correctly identifies two identical Spark DataFrames containing rows with null values as equal, both when ignoring and respecting row order.

#### `test_assert_equal_timestamp`
This test verifies that `assertDataFrameEqual` correctly identifies two identical Spark DataFrames containing timestamp values as equal, both when ignoring and respecting row order.

#### `test_assert_error_non_pyspark_df`
This test verifies that `assertDataFrameEqual` raises a specific `PySparkAssertionError` (INVALID_TYPE_DF_EQUALITY_ARG) when passed arguments that are not valid PySpark DataFrame-like objects (in this case, regular Python dictionaries), ensuring type safety.

#### `test_assert_error_pandas_pyspark_df`
This test verifies that `assertDataFrameEqual` raises a specific `PySparkAssertionError` (INVALID_TYPE_DF_EQUALITY_ARG) when trying to compare a Pandas-on-Spark DataFrame with a native Spark DataFrame, as they are not directly comparable.

#### `test_assert_notequal_arraytype`
This test verifies that `assertDataFrameEqual` correctly identifies differences in Spark DataFrames containing array types and raises a `PySparkAssertionError` with detailed context diff, both when ignoring and respecting row order.

#### `test_assert_notequal_nullval`
This test verifies that `assertDataFrameEqual` correctly identifies differences where one DataFrame has a null value and other has a non-null value, raising a `PySparkAssertionError` with detailed context diff.

#### `test_assert_notequal_schema`
This test verifies that `assertDataFrameEqual` correctly identifies differences in schema (column names and types) between two Spark DataFrames and raises a `PySparkAssertionError` with a diff of the schemas.

#### `test_assert_pyspark_approx_equal`
This test verifies that `assertDataFrameEqual` considers floating-point values that differ by a very small amount as equal by default (approximate equality), both when ignoring and respecting row order.

#### `test_assert_pyspark_approx_equal_custom_rtol`
This test verifies that `assertDataFrameEqual` can handle a custom relative tolerance (`rtol`) for comparing floating-point numbers, considering values that fall within that tolerance as equal.

#### `test_assert_pyspark_df_not_equal`
This test verifies that `assertDataFrameEqual` correctly identifies differences in floating-point values that exceed default tolerance and raises a `PySparkAssertionError` with detailed context diff.

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

#### `test_assert_schema_equal_with_decimal_types`
Test assertSchemaEqual with decimal types of different precision and scale
(SPARK-51062).

#### `test_assert_type_error_pandas_df`
This test verifies that `assertDataFrameEqual` raises a specific `PySparkAssertionError` (DIFFERENT_PANDAS_DATAFRAME) when trying to compare a Pandas-on-Spark DataFrame with a standard Pandas DataFrame.

#### `test_assert_unequal_null_actual`
*No description available.*

#### `test_assert_unequal_null_expected`
*No description available.*

#### `test_assert_unequal_pandas_df`
*No description available.*

#### `test_capture_analysis_exception`
This test verifies that specific operations like querying unresolved columns or applying invalid expressions correctly raise an `AnalysisException`.

#### `test_capture_illegalargument_exception`
This test verifies that setting invalid configuration values (like negative number of reduce tasks) correctly raises an `IllegalArgumentException` with a specific error message.

#### `test_capture_parse_exception`
This test verifies that invalid SQL syntax correctly raises a `ParseException`.

#### `test_capture_pyspark_value_exception`
This test verifies that passing invalid parameter values to PySpark functions (like invalid number of bits to `sha2`) correctly raises a `PySparkValueError`.

#### `test_capture_user_friendly_exception`
This test verifies that an `AnalysisException` raised for an unresolved column (in this case, with Chinese characters) contains a specific, user-friendly error class (`UNRESOLVED_COLUMN`) and correctly identifies the problematic column name in the error message.

#### `test_check_row_order_error`
This test verifies that `assertDataFrameEqual` correctly identifies a difference in row order when `checkRowOrder=True` is specified, raising a `PySparkAssertionError` with a detailed context diff.

#### `test_dataframe_ignore_column_name`
This test verifies behavior of `ignoreColumnName` flag in `assertDataFrameEqual`. It ensures that test fails if column names differ and the flag is false, but passes when the flag is set to true.

#### `test_dataframe_ignore_column_order`
This test verifies behavior of `ignoreColumnOrder` flag in `assertDataFrameEqual`. It ensures that test fails if column order differs and the flag is false, but passes when the flag is set to true.

#### `test_dataframe_ignore_column_type`
This test verifies behavior of `ignoreColumnType` flag in `assertDataFrameEqual`. It ensures that test fails if column types differ and the flag is false, but passes when the flag is set to true.

#### `test_dataframe_include_diff_rows`
This test verifies that setting `includeDiffRows=True` in `assertDataFrameEqual` causes the resulting `PySparkAssertionError` to contain a structured list of the differing rows in its `data` property, enabling programmatic analysis of the failures.

#### `test_dataframe_max_errors`
This test verifies that the `maxErrors` parameter in `assertDataFrameEqual` effectively limits the number of mismatched rows reported in the error message to the specified maximum.

#### `test_dataframe_show_only_diff`
This test verifies behavior of `showOnlyDiff` flag in `assertDataFrameEqual`. It ensures that when true, the generated error message only includes information about rows that are different, excluding matching rows, whereas when false, it includes all rows.

#### `test_df_list_row_equal`
This test verifies that `assertDataFrameEqual` can compare a Spark DataFrame with a list of PySpark `Row` objects, treating them as equal if they contain same data, both when ignoring and respecting row order.

#### `test_diff_schema_lens`
This test verifies that `assertDataFrameEqual` correctly identifies when two DataFrames have a different number of columns and raises a `PySparkAssertionError` with a diff of the schemas.

#### `test_empty_dataset`
This test verifies that converting an empty Spark SQL query result to a Pandas DataFrame works correctly in Spark Connect, resulting in an empty Pandas DataFrame with correct column names.

#### `test_empty_expected_list`
This test verifies that `assertDataFrameEqual` correctly identifies equality when comparing a DataFrame that has no columns but contains rows, against a list of empty PySpark `Row` objects, both when ignoring and respecting row order.

#### `test_empty_no_column`
This test verifies that `assertDataFrameEqual` correctly identifies two empty DataFrames with no columns as equal, both when ignoring and respecting row order.

#### `test_empty_no_column_expected_list`
This test verifies that `assertDataFrameEqual` correctly identifies an empty DataFrame with no columns as equal to an empty list, both when ignoring and respecting row order.

#### `test_get_error_class_state`
This test verifies that `AnalysisException` correctly exposes error condition, SQL state, message parameters, and query context according to the SparkThrowable interface, and handles cases without error conditions correctly.

#### `test_list_row_unequal_schema`
This test verifies that `assertDataFrameEqual` raises `DIFFERENT_ROWS` error when comparing a Spark DataFrame with a list of `Row` objects that have matching structure but different data types (e.g., integer vs string), raising a `PySparkAssertionError` with a detailed context diff.

#### `test_list_rows_equal`
This test verifies that `assertDataFrameEqual` can compare two lists of PySpark `Row` objects and identify them as equal, both when ignoring and respecting row order.

#### `test_list_rows_unequal`
This test verifies that `assertDataFrameEqual` correctly identifies differences between two lists of PySpark `Row` objects and raises a `PySparkAssertionError` with a detailed context diff, both when ignoring and respecting row order.

#### `test_no_column`
This test verifies that `assertDataFrameEqual` correctly identifies two DataFrames with no columns but containing rows as equal, both when ignoring and respecting row order.

#### `test_no_column_expected_list`
This test verifies that `assertDataFrameEqual` correctly identifies an empty DataFrame as equal to an empty list, both when ignoring and respecting row order.

#### `test_remove_non_word_characters_long`
This test verifies correctness of a custom string processing operation (removing non-word characters using regular expression) by comparing the processed DataFrame with an expected DataFrame containing the correct results.

#### `test_row_order_ignored`
This test verifies that `assertDataFrameEqual` ignores row order by default when comparing two Spark DataFrames with the same data in different order.

#### `test_schema_array_unequal`
This test verifies that `assertSchemaEqual` correctly identifies differences in complex schemas (specifically array types with different element types and nullability) and raises a `PySparkAssertionError` with a diff of the schemas.

#### `test_schema_ignore_nullable`
This test verifies behavior of `ignoreNullable` flag in `assertDataFrameEqual`. It ensures that test fails if schema nullability differs and the flag is false, but passes when the flag is set to true.

#### `test_schema_ignore_nullable_array_equal`
This test verifies that `assertSchemaEqual` considers two schemas equal even if they differ in nullability for an array field and its elements. Schema `s1` has an array field that is nullable and contains nullable elements, while `s2` has an array field that is non-nullable and contains non-nullable elements. Both have the same base type (DoubleType).

#### `test_schema_ignore_nullable_map_unequal`
This test verifies that `assertSchemaEqual` raises a `PySparkAssertionError` when comparing two schemas with different map key/value types, even when `ignoreNullable=True` is specified. Schema `s1` has a map from string to integer, while `s2` has a map from integer to string.

#### `test_schema_ignore_nullable_struct_equal`
This test verifies that `assertSchemaEqual` considers two schemas equal even if they differ in nullability for a nested struct field and its inner field. Schema `s1` has a struct field that is nullable and contains a nullable integer field, while `s2` has a struct field that is non-nullable and contains a non-nullable integer field.

#### `test_schema_more_nested_struct_unequal`
This test verifies that `assertSchemaEqual` raises a `PySparkAssertionError` with a specific error class ("DIFFERENT_SCHEMA") and a detailed diff message when comparing two complex nested schemas that differ in a nested field type. In this case, the `middlename` field is a `StringType` in the first schema and a `BooleanType` in the second.

#### `test_schema_struct_unequal`
This test verifies that `assertSchemaEqual` raises a `PySparkAssertionError` with error class "DIFFERENT_SCHEMA" and a diff message when comparing two schemas where a field in a nested struct has a different type. Specifically, the `age` field in the nested struct is `DoubleType` in the first schema and `IntegerType` in the second.

#### `test_schema_unsupported_type`
This test verifies that `assertSchemaEqual` raises a `PySparkTypeError` with error class "NOT_EXPECTED_TYPE" when called with arguments that are not supported schema types (strings instead of `StructType`). It checks that the error parameters correctly identify that a 'struct type' was expected but a 'str' was provided.

#### `test_spark_sql`
This test verifies that `assertDataFrameEqual` correctly identifies that two DataFrames produced by different SQL queries are equal when they contain the same data. It performs this check both with and without enforcing row order (`checkRowOrder=True`).

#### `test_spark_sql_sort_rows`
This test verifies that `assertDataFrameEqual` can handle DataFrames with different row orders by default, and that it correctly enforces row order when `checkRowOrder=True` is specified. It creates two DataFrames with the same content but different row order, runs a sorted query on one and a non-sorted query on the other, and asserts equality.

#### `test_spark_upgrade_exception`
This test verifies that PySpark handles a `SparkUpgradeException` correctly when collecting a DataFrame. The exception is triggered by attempting to convert a string with an invalid date format ("2014-31-12") using `from_unixtime` and `to_date` with a specific pattern ("yyyy-dd-aa").

#### `test_special_vals`
This test verifies that `assertDataFrameEqual` correctly handles DataFrames containing special floating-point values like NaN (Not a Number), positive infinity, and negative infinity. It asserts equality both with and without checking row order.

### Class: `ReusedConnectTestCase`

#### `test_assert_remote_mode`
*Note: Could not find definition in parity or base files.*

## File: `test_local_dummy.py`

### Class: `LocalDummyTest`

#### `test_dummy`
A dummy test that always asserts true.

