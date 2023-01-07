#[cfg(test)]
mod construct_tests {
    extern crate apache_beam;

    use apache_beam::construct::build_pipeline;
    use apache_beam::construct::PCollection;
    use apache_beam::construct::PTransform;
    use apache_beam::construct::Root;
    use apache_beam::transforms::Impulse;

    use apache_beam::runners::direct_runner::Runner;
    use apache_beam::runners::direct_runner::DirectRunner;

    #[test]
    fn apply_impulse() {
        let proto = build_pipeline(&|root: Root| {
            root.apply(&"impulse".to_string(), &Impulse {});
        });
        println!("{:#?}", proto);
    }

    #[test]
    fn apply_composite() {
        let proto = build_pipeline(&|root: Root| {
            // TODO: That's a lot of boilerplate with the lifetime annotations,
            // and they don't seem to be buying us much. Can we get rid of them?
            // (See other comment about storing an Rc<PipelineHandler> rather than
            // a bare ref...)
            struct RootComposite {}
            impl<'a> PTransform<'a, Root<'a>, PCollection<'a, String>> for RootComposite {
                fn expand(&self, root: &Root<'a>) -> PCollection<'a, String> {
                    root.apply(&"Inner".to_string(), &Impulse {});
                    root.apply(&"Returned".to_string(), &Impulse {})
                }
            }

            struct EmptyComposite {}
            impl<'a> PTransform<'a, PCollection<'a, String>, PCollection<'a, String>> for EmptyComposite {
                fn expand(&self, pcoll: &PCollection<'a, String>) -> PCollection<'a, String> {
                    pcoll.clone()
                }
            }

            root.apply(&"Root".to_string(), &RootComposite {})
                .apply(&"Empty".to_string(), &EmptyComposite {});
        });

        //println!("{:#?}", proto);
        assert_eq!(proto.root_transform_ids.len(), 2);
        let transforms = &proto.components.as_ref().unwrap().transforms;
        assert_eq!(transforms.len(), 4);

        let transform0 = transforms.get("transform0").as_ref().unwrap().clone();
        let transform2 = transforms.get("transform2").as_ref().unwrap().clone();
        let transform3 = transforms.get("transform3").as_ref().unwrap().clone();

        assert_eq!(transform0.outputs.get("out").unwrap(), "pcoll1");
        assert_eq!(transform0.unique_name, "Root");
        assert_eq!(transform0.subtransforms.len(), 2);

        assert_eq!(transform2.outputs.get("out").unwrap(), "pcoll1");
        assert_eq!(transform2.unique_name, "Root/Returned");
        assert_eq!(transform2.subtransforms.len(), 0);

        assert_eq!(transform3.inputs.get("pcoll1").unwrap(), "pcoll1");
        assert_eq!(transform3.unique_name, "Empty");
    }
    
    fn do_fn(s: &String) -> Vec<String> {
        vec![s.clone(), "hello".to_string(), s.clone()]
    }

    // cargo test construct_tests::flat_map -- --nocapture
    #[test]
    fn flat_map() {
        println!("\nhello!\n");
        let proto = build_pipeline(&|root: Root| {
            root.apply(&"impulse".to_string(), &Impulse {})
                .flat_map(&"Empty".to_string(), do_fn);
        });

        println!("{:#?}", proto);
        
        println!("\nhello!\n");
    }

    #[test]
    fn direct_runner() -> Result<(), String> {
        let dr = DirectRunner {};
        dr.run(&|root: Root| {
            root.apply(&"impulse".to_string(), &Impulse {});
        })?;
        Ok(())
    }
}
