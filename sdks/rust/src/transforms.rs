use crate::construct::{PCollection, PTransform, PipelineHolder, Root};
use crate::worker::operators::beam_api::org::apache::beam::model::pipeline::v1 as proto;
use crate::worker::operators::PAR_DO_URN;
use crate::worker::operators::{GBK_URN, IMPULSE_URN};

// Create would be a PTransform<Root, PCollection<T>> that simply implements
// expand to call Impulse + FlatMap.
// Other composites could be created as well.

// GroupByKey should be just like Impulse. It should be a
// PTransform<PCollection<(K, V)>, PCollection<(K, Vec<V>)>>
// For now, we have to store a KeyExtractor in the payload of GroupByKey.
// But we can create one and register it (maybe using the same serialization
// mechanisms) with the concrete types we'll have when this operator
// is instantiated.
// In the very short term, only allow StringString, StringI32, and StringI64.

pub struct Impulse {}

impl<'x> PTransform<'x, Root<'x>, PCollection<'x, String>> for Impulse {
    fn expand(&self, _input: &Root<'x>) -> PCollection<'x, String> {
        panic!("TODO: Provide default impl when exandInternal implemented.");
    }
    fn expand_internal(
        &self,
        _input: &Root<'x>,
        pipeline: &'x PipelineHolder,
        transform_proto: &mut proto::PTransform,
    ) -> PCollection<'x, String> {
        // Update the spec to say how it's created.
        transform_proto.spec = Some(proto::FunctionSpec {
            urn: IMPULSE_URN.to_string(),
            payload: vec![],
        });
        // Created out of thin air.
        pipeline.create_pcollection_internal()
    }
}

pub struct Gbk {
    /// One of [StringString, StringInt32, StringInt64] for now.
    tuple_type: String,
}

impl<'x, K, V> PTransform<'x, PCollection<'x, (K, V)>, PCollection<'x, (K, Vec<V>)>> for Gbk {
    fn expand(&self, _input: &PCollection<'x, (K, V)>) -> PCollection<'x, (K, Vec<V>)> {
        panic!("TODO: Provide default impl when expandInternal implemented.");
    }

    fn expand_internal(
        &self,
        _input: &PCollection<'x, (K, V)>,
        _pipeline: &'x PipelineHolder,
        _transform_proto: &mut proto::PTransform,
    ) -> PCollection<'x, (K, Vec<V>)> {
        _transform_proto.spec = Some(proto::FunctionSpec {
            urn: GBK_URN.to_string(),
            payload: self.tuple_type.to_string().into_bytes(),
        });
        _pipeline.create_pcollection_internal()
    }
}

// For demo purposes, we could consider adding a "primitive" ReadFromText/WriteToText
// and its corresponding Operator. The Read's start() would open the file and
// pass all lines to its consumers, and Write would open/write/close in its
// start(), process(), finish() methods.

pub struct FlatMapTransform {
    pub payload: String,
}

impl<'x, T: 'static, O: 'static> PTransform<'x, PCollection<'x, T>, PCollection<'x, O>>
    for FlatMapTransform
{
    fn expand(&self, _input: &PCollection<'x, T>) -> PCollection<'x, O> {
        panic!("TODO: Provide default impl when exandInternal implemented.");
    }
    fn expand_internal(
        &self,
        _input: &PCollection<'x, T>,
        pipeline: &'x PipelineHolder,
        transform_proto: &mut proto::PTransform,
    ) -> PCollection<'x, O> {
        // Update the spec to say how it's created.
        transform_proto.spec = Some(proto::FunctionSpec {
            urn: PAR_DO_URN.to_string(),
            payload: self.payload.clone().into(),
        });
        // TODO: is this right?
        pipeline.create_pcollection_internal()
    }
}
