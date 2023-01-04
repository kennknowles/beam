use std::any::Any;
use std::collections::HashMap;
use std::iter::Iterator;

use std::rc::Rc;


//use beam_api::org::apache::beam::model::pipeline::v1::Pipeline;
use beam_api::org::apache::beam::model::fn_execution::v1::ProcessBundleDescriptor;

pub mod beam_api {
    tonic::include_proto!("beam_api");
}

type SomeError = String;

pub trait Operator {
    fn start(&self) -> Result<(), SomeError>;
    fn process(&self, element: &dyn Any) -> Result<(), SomeError>;
    fn finish(&self) -> Result<(), SomeError>;
}

struct DataSourceOperator {
    //consumers: Vec<Box<dyn Operator>>,
}

impl Operator for DataSourceOperator {
    fn start(&self) -> Result<(), SomeError> {
        Ok(())
    }
    fn process(&self, _element: &dyn Any) -> Result<(), SomeError> {
        return Err("Data sources should never have inputs.".to_string())
    }
    fn finish(&self) -> Result<(), SomeError> {
        Ok(())
    }
}

struct DataSinkOperator {}

impl Operator for DataSinkOperator {
    fn start(&self) -> Result<(), SomeError> {
        Ok(())
    }
    fn process(&self, _element: &dyn Any) -> Result<(), SomeError> {
        Ok(())
    }
    fn finish(&self) -> Result<(), SomeError> {
        Ok(())
    }
}

struct DoOperator<'a> {
    consumers: Vec<&'a dyn Operator>,
}

impl<'a> Operator for DoOperator<'a> {
    fn start(&self) -> Result<(), SomeError> {
        Ok(())
    }
    fn process(&self, element: &dyn Any) -> Result<(), SomeError> {
        for consumer in self.consumers.iter() {
          consumer.process(element)?;
        }
        Ok(())
    }
    fn finish(&self) -> Result<(), SomeError> {
        Ok(())
    }
}

pub fn create_bundle_processor<'a>(bundle_descriptor: &ProcessBundleDescriptor) -> BundleProcessor {
  let mut operators_by_id = HashMap::new();
  let mut rev_topo_order = Vec::new();

  let create_operator = |transform_id: &String| -> Rc<dyn Operator> {
    return Rc::new(DoOperator{consumers: [].to_vec()});
  };

  let get_operator = |transform_id: &String| -> Rc<dyn Operator> {
    if !operators_by_id.contains_key(transform_id) {
      operators_by_id.insert(transform_id.to_string(), create_operator(transform_id));
      rev_topo_order.push(transform_id.to_string());
    }
    return operators_by_id.get(transform_id).unwrap().clone();
  };

  let mut operators = Vec::new();
  for transform_id in rev_topo_order.iter().rev() {
    operators.push(operators_by_id.get(transform_id).unwrap().clone());
  }

  BundleProcessor{operators: operators, operators_by_id: operators_by_id}
}

pub struct BundleProcessor {
    //bundle_descriptor: ProcessBundleDescriptor,
    operators: Vec<Rc<dyn Operator>>,
    operators_by_id: HashMap<String, Rc<dyn Operator>>
}

impl BundleProcessor {
    fn start(&self) -> Result<(), SomeError> {
        for operator in self.operators.iter().rev() {
            operator.start()?;
        }

        Ok(())
    }

    fn finish(&self) -> Result<(), SomeError> {
        for operator in self.operators.iter() {
            operator.start()?;
        }

        Ok(())
    }
}
