#[cfg(test)]
mod trigger_index_tests {
    use crate::catalog::Catalog;
    use crate::parser::ast::{
        CreateIndexStmt, CreateTriggerStmt, TriggerEvent, TriggerFor, TriggerTiming,
    };

    #[test]
    fn test_create_and_get_trigger() {
        let catalog = Catalog::new();
        let trigger_name = "my_trigger";
        let trigger = CreateTriggerStmt {
            name: trigger_name.to_string(),
            timing: TriggerTiming::Before,
            event: TriggerEvent::Insert,
            table: "users".to_string(),
            for_each: TriggerFor::EachRow,
            when: None,
            body: Vec::new(),
        };

        assert!(catalog.create_trigger(trigger.clone()).is_ok());
        let fetched = catalog.get_trigger(trigger_name).unwrap();
        assert_eq!(fetched.name, trigger.name);
    }

    #[test]
    fn test_drop_trigger() {
        let catalog = Catalog::new();
        let trigger_name = "my_trigger_to_drop";
        let trigger = CreateTriggerStmt {
            name: trigger_name.to_string(),
            timing: TriggerTiming::After,
            event: TriggerEvent::Delete,
            table: "products".to_string(),
            for_each: TriggerFor::EachStatement,
            when: None,
            body: Vec::new(),
        };
        catalog.create_trigger(trigger).unwrap();
        assert!(catalog.get_trigger(trigger_name).is_some());
        assert!(catalog.drop_trigger(trigger_name, false).is_ok());
        assert!(catalog.get_trigger(trigger_name).is_none());
    }

    #[test]
    fn test_create_and_get_index() {
        let catalog = Catalog::new();
        let index_name = "my_index";
        let index = CreateIndexStmt {
            name: index_name.to_string(),
            table: "users".to_string(),
            columns: vec!["id".to_string()],
            expressions: vec![],
            unique: true,
            where_clause: None,
        };

        assert!(catalog.create_index(index.clone()).is_ok());
        let fetched = catalog.get_index(index_name).unwrap();
        assert_eq!(fetched.name, index.name);
    }

    #[test]
    fn test_drop_index() {
        let catalog = Catalog::new();
        let index_name = "my_index_to_drop";
        let index = CreateIndexStmt {
            name: index_name.to_string(),
            table: "orders".to_string(),
            columns: vec!["order_date".to_string()],
            expressions: vec![],
            unique: false,
            where_clause: None,
        };
        catalog.create_index(index).unwrap();
        assert!(catalog.get_index(index_name).is_some());
        assert!(catalog.drop_index(index_name, false).is_ok());
        assert!(catalog.get_index(index_name).is_none());
    }
}
