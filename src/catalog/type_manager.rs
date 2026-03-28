use crate::parser::ast::{CompositeTypeDef, DataType, EnumTypeDef};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

pub struct TypeManager {
    pub(crate) enum_types: Arc<RwLock<HashMap<String, EnumTypeDef>>>,
    pub(crate) composite_types: Arc<RwLock<HashMap<String, CompositeTypeDef>>>,
    pub(crate) tables: Arc<RwLock<HashMap<String, crate::catalog::TableSchema>>>,
    pub(crate) catalog: Arc<crate::catalog::Catalog>,
}

impl TypeManager {
    pub fn new() -> Self {
        Self {
            enum_types: Arc::new(RwLock::new(HashMap::new())),
            composite_types: Arc::new(RwLock::new(HashMap::new())),
            tables: Arc::new(RwLock::new(HashMap::new())),
            catalog: Arc::new(crate::catalog::Catalog::new()),
        }
    }

    pub fn with_types(
        enum_types: Arc<RwLock<HashMap<String, EnumTypeDef>>>,
        composite_types: Arc<RwLock<HashMap<String, CompositeTypeDef>>>,
        tables: Arc<RwLock<HashMap<String, crate::catalog::TableSchema>>>,
        catalog: Arc<crate::catalog::Catalog>,
    ) -> Self {
        Self { enum_types, composite_types, tables, catalog }
    }

    pub fn create_type(&self, type_name: String, labels: Vec<String>) -> Result<(), String> {
        if labels.is_empty() {
            return Err(format!("Enum type '{}' must have at least one label", type_name));
        }

        let mut enum_types = self.enum_types.write().unwrap();

        if enum_types.contains_key(&type_name) {
            return Err(format!("Type '{}' already exists", type_name));
        }

        if self.composite_types.read().unwrap().contains_key(&type_name) {
            return Err(format!("Type '{}' already exists as composite type", type_name));
        }

        let def = EnumTypeDef { type_name: type_name.clone(), labels };
        enum_types.insert(type_name, def);
        Ok(())
    }

    pub fn create_composite_type(
        &self,
        type_name: String,
        fields: Vec<(String, DataType)>,
    ) -> Result<(), String> {
        if fields.is_empty() {
            return Err(format!("Composite type '{}' must have at least one field", type_name));
        }

        let mut composite_types = self.composite_types.write().unwrap();

        if composite_types.contains_key(&type_name) {
            return Err(format!("Type '{}' already exists", type_name));
        }

        if self.enum_types.read().unwrap().contains_key(&type_name) {
            return Err(format!("Type '{}' already exists as enum type", type_name));
        }

        let mut seen_names: HashSet<&String> = HashSet::new();
        for (name, _) in &fields {
            if !seen_names.insert(name) {
                return Err(format!(
                    "Composite type '{}' cannot have duplicate field names",
                    type_name
                ));
            }
        }

        let def = CompositeTypeDef { type_name: type_name.clone(), fields };
        composite_types.insert(type_name, def);
        Ok(())
    }

    pub fn get_composite_type(&self, type_name: &str) -> Option<CompositeTypeDef> {
        self.composite_types.read().unwrap().get(type_name).cloned()
    }

    pub fn drop_type(&self, type_name: &str, if_exists: bool, cascade: bool) -> Result<(), String> {
        {
            let enum_types = self.enum_types.read().unwrap();
            let composite_types = self.composite_types.read().unwrap();
            if !enum_types.contains_key(type_name) && !composite_types.contains_key(type_name) {
                if if_exists {
                    return Ok(());
                }
                return Err(format!("Type '{}' does not exist", type_name));
            }
        }

        let dependent_tables: Vec<String> = {
            let tables = self.tables.read().unwrap();
            let mut deps = Vec::new();
            for (table_name, schema) in tables.iter() {
                for col in &schema.columns {
                    match col.data_type {
                        DataType::Enum(ref t) if t == type_name => {
                            deps.push(table_name.clone());
                            break;
                        }
                        DataType::Composite(ref t) if t == type_name => {
                            deps.push(table_name.clone());
                            break;
                        }
                        _ => {}
                    }
                }
            }
            deps
        };

        if !dependent_tables.is_empty() && !cascade {
            return Err(format!(
                "cannot drop type '{}' because it is used by table column",
                type_name
            ));
        }

        if cascade {
            for table_name in dependent_tables {
                self.catalog.drop_table(&table_name, false)?;
            }
        }

        {
            let mut enum_types = self.enum_types.write().unwrap();
            enum_types.remove(type_name);
        }
        {
            let mut composite_types = self.composite_types.write().unwrap();
            composite_types.remove(type_name);
        }
        Ok(())
    }

    pub fn alter_type_add_value(
        &self,
        type_name: &str,
        new_label: String,
        after_label: Option<String>,
    ) -> Result<(), String> {
        let mut enum_types = self.enum_types.write().unwrap();

        let def = enum_types
            .get_mut(type_name)
            .ok_or_else(|| format!("Type '{}' does not exist", type_name))?;

        if def.labels.contains(&new_label) {
            return Err(format!(
                "Enum label '{}' already exists in type '{}'",
                new_label, type_name
            ));
        }

        match after_label {
            Some(after) => {
                let pos = def.labels.iter().position(|l| l == &after).ok_or_else(|| {
                    format!("Label '{}' does not exist in enum '{}'", after, type_name)
                })?;
                def.labels.insert(pos + 1, new_label);
            }
            None => {
                def.labels.push(new_label);
            }
        }
        Ok(())
    }

    pub fn get_enum_type(&self, type_name: &str) -> Option<EnumTypeDef> {
        self.enum_types.read().unwrap().get(type_name).cloned()
    }

    pub fn get_enum_label_index(&self, type_name: &str, label: &str) -> Option<i32> {
        self.enum_types
            .read()
            .unwrap()
            .get(type_name)
            .and_then(|def| def.labels.iter().position(|l| l == label))
            .map(|pos| pos as i32)
    }
}
