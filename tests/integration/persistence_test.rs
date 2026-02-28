use rustgres::catalog::Catalog;
use rustgres::parser::{Parser, Statement};
use tempfile::TempDir;

#[test]
fn test_view_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();
    
    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        
        let mut parser = Parser::new("CREATE VIEW v AS SELECT * FROM t").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateView(create) => {
                catalog.create_view(create.name.clone(), *create.query).unwrap();
            }
            _ => panic!("Expected CREATE VIEW"),
        }
    }
    
    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        assert!(catalog.get_view("v").is_some());
    }
}

#[test]
fn test_trigger_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();
    
    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        
        let mut parser = Parser::new("CREATE TRIGGER t BEFORE INSERT ON users FOR EACH ROW BEGIN END").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateTrigger(create) => {
                catalog.create_trigger(create).unwrap();
            }
            _ => panic!("Expected CREATE TRIGGER"),
        }
    }
    
    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        assert!(catalog.get_trigger("t").is_some());
    }
}

#[test]
fn test_multiple_views_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();
    
    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        
        for i in 0..5 {
            let sql = format!("CREATE VIEW v{} AS SELECT * FROM t{}", i, i);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            
            match stmt {
                Statement::CreateView(create) => {
                    catalog.create_view(create.name.clone(), *create.query).unwrap();
                }
                _ => panic!("Expected CREATE VIEW"),
            }
        }
    }
    
    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        for i in 0..5 {
            assert!(catalog.get_view(&format!("v{}", i)).is_some());
        }
    }
}

#[test]
fn test_multiple_triggers_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();
    
    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        
        for i in 0..5 {
            let sql = format!("CREATE TRIGGER t{} BEFORE INSERT ON users FOR EACH ROW BEGIN END", i);
            let mut parser = Parser::new(&sql).unwrap();
            let stmt = parser.parse().unwrap();
            
            match stmt {
                Statement::CreateTrigger(create) => {
                    catalog.create_trigger(create).unwrap();
                }
                _ => panic!("Expected CREATE TRIGGER"),
            }
        }
    }
    
    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        for i in 0..5 {
            assert!(catalog.get_trigger(&format!("t{}", i)).is_some());
        }
    }
}

#[test]
fn test_view_drop_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();
    
    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        
        let mut parser = Parser::new("CREATE VIEW v AS SELECT * FROM t").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateView(create) => {
                catalog.create_view(create.name.clone(), *create.query).unwrap();
            }
            _ => panic!("Expected CREATE VIEW"),
        }
        
        catalog.drop_view("v", false).unwrap();
    }
    
    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        assert!(catalog.get_view("v").is_none());
    }
}

#[test]
fn test_trigger_drop_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap();
    
    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        
        let mut parser = Parser::new("CREATE TRIGGER t BEFORE INSERT ON users FOR EACH ROW BEGIN END").unwrap();
        let stmt = parser.parse().unwrap();
        
        match stmt {
            Statement::CreateTrigger(create) => {
                catalog.create_trigger(create).unwrap();
            }
            _ => panic!("Expected CREATE TRIGGER"),
        }
        
        catalog.drop_trigger("t", false).unwrap();
    }
    
    {
        let catalog = Catalog::new_with_data_dir(data_dir);
        assert!(catalog.get_trigger("t").is_none());
    }
}
