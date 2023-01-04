use std::any::Any;
use std::borrow::Borrow;
use std::boxed::Box;
use std::collections::HashMap;
use std::iter::Iterator;


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

  let create_operator = |transform_id: &String| -> Box<dyn Operator> {
    return Box::new(DoOperator{consumers: [].to_vec()});
  };

  let ensure_operator = |transform_id: &String| { //-> &'a dyn Operator{
    if !operators_by_id.contains_key(transform_id) {
      operators_by_id.insert(transform_id.to_string(), create_operator(transform_id));
      rev_topo_order.push(transform_id.to_string());
    }
    //return operators_by_id.get(transform_id).unwrap().borrow();
  };

  // rev_topo_order.iter().rev().map(|transform_id| { operators_by_id.get(transform_id).unwrap().borrow() })
  let mut topo_order = Vec::new();
  for transform_id in rev_topo_order.iter().rev() {
    topo_order.push(transform_id.to_string());
  }

  BundleProcessor::new(operators_by_id.into(), &topo_order)
}

pub struct BundleProcessor {
    //bundle_descriptor: ProcessBundleDescriptor,
    operators: Vec<String>, //Vec<&'a dyn Operator>,
    operators_by_id: HashMap<String, Box<dyn Operator>>
}

impl BundleProcessor {
    pub fn new(operators_by_id: HashMap<String, Box<dyn Operator>>, topo_order: &Vec<String>) -> Self {
      let mut result = Self { operators: [].to_vec(), operators_by_id: operators_by_id };
      result.populate_operators(topo_order);
      result
    }

    fn populate_operators(&mut self, topo_order: &Vec<String>) {
      self.operators = topo_order.to_vec();
      /*
      for transform_id in topo_order {
        self.operators.push(self.operators_by_id.get(transform_id).unwrap().borrow());
      }
      */
    }

    fn start(&self) -> Result<(), SomeError> {
        let mut self_operators : Vec<&dyn Operator> = Vec::new();
        for transform_id in self.operators.iter() {
          self_operators.push(self.operators_by_id.get(transform_id).unwrap().borrow());
        }

        for operator in self_operators.iter().rev() {
            operator.start()?;
        }

        Ok(())
    }

    fn finish(&self) -> Result<(), SomeError> {
        let mut self_operators : Vec<&dyn Operator> = Vec::new();
        for transform_id in self.operators.iter() {
          self_operators.push(self.operators_by_id.get(transform_id).unwrap().borrow());
        }

        for operator in self_operators.iter() {
            operator.start()?;
        }

        Ok(())
    }
}
