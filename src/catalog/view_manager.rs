use super::Value;
use super::crud_helper::CrudHelper;
use crate::parser::ast::SelectStmt;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct ViewManager {
    pub(crate) views: Arc<RwLock<HashMap<String, SelectStmt>>>,
    pub(crate) materialized_views:
        Arc<RwLock<HashMap<String, (SelectStmt, Vec<Vec<Value>>, Vec<String>)>>>,
    pub(crate) catalog: Arc<crate::catalog::Catalog>,
}

impl ViewManager {
    pub fn new() -> Self {
        Self {
            views: Arc::new(RwLock::new(HashMap::new())),
            materialized_views: Arc::new(RwLock::new(HashMap::new())),
            catalog: Arc::new(crate::catalog::Catalog::new()),
        }
    }

    pub fn with_views(
        views: Arc<RwLock<HashMap<String, SelectStmt>>>,
        materialized_views: Arc<
            RwLock<HashMap<String, (SelectStmt, Vec<Vec<Value>>, Vec<String>)>>,
        >,
        catalog: Arc<crate::catalog::Catalog>,
    ) -> Self {
        Self { views, materialized_views, catalog }
    }

    pub fn create_view(&self, name: String, query: SelectStmt) -> Result<(), String> {
        CrudHelper::create(&self.views, name, query, "View")
    }

    pub fn drop_view(&self, name: &str, if_exists: bool) -> Result<(), String> {
        CrudHelper::drop(&self.views, name, if_exists, "View")
    }

    pub fn get_view(&self, name: &str) -> Option<SelectStmt> {
        CrudHelper::get(&self.views, name)
    }

    pub fn create_materialized_view(&self, name: String, query: SelectStmt) -> Result<(), String> {
        let mut mvs = self.materialized_views.write().unwrap();

        if mvs.contains_key(&name) {
            return Err(format!("Materialized view '{}' already exists", name));
        }

        use crate::planner::planner::Planner;
        let planner = Planner::new_with_catalog(self.catalog.clone());
        let mut plan = planner.plan(&query).map_err(|e| format!("{:?}", e))?;

        let mut data: Vec<Vec<Value>> = Vec::new();
        let mut column_names: Vec<String> = Vec::new();

        loop {
            match plan.next() {
                Ok(Some(tuple_hashmap)) => {
                    if column_names.is_empty() {
                        let mut names: Vec<String> = tuple_hashmap.keys().cloned().collect();
                        names.sort();
                        column_names = names;
                    }
                    let row: Vec<Value> = column_names
                        .iter()
                        .map(|col| tuple_hashmap.get(col).cloned().unwrap_or(Value::Null))
                        .collect();
                    data.push(row);
                }
                Ok(None) => break,
                Err(e) => return Err(format!("{:?}", e)),
            }
        }

        mvs.insert(name, (query, data, column_names));
        Ok(())
    }

    pub fn refresh_materialized_view(&self, name: &str) -> Result<(), String> {
        let mut mvs = self.materialized_views.write().unwrap();

        let (query, data, column_names) = mvs
            .get_mut(name)
            .ok_or_else(|| format!("Materialized view '{}' does not exist", name))?;

        use crate::planner::planner::Planner;
        let planner = Planner::new_with_catalog(self.catalog.clone());
        let mut plan = planner.plan(query).map_err(|e| format!("{:?}", e))?;

        let mut new_data: Vec<Vec<Value>> = Vec::new();
        loop {
            match plan.next() {
                Ok(Some(tuple_hashmap)) => {
                    let row: Vec<Value> = column_names
                        .iter()
                        .map(|col| tuple_hashmap.get(col).cloned().unwrap_or(Value::Null))
                        .collect();
                    new_data.push(row);
                }
                Ok(None) => break,
                Err(e) => return Err(format!("{:?}", e)),
            }
        }

        *data = new_data;
        Ok(())
    }

    pub fn drop_materialized_view(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let mut mvs = self.materialized_views.write().unwrap();

        if mvs.remove(name).is_none() && !if_exists {
            return Err(format!("Materialized view '{}' does not exist", name));
        }
        Ok(())
    }

    pub fn get_materialized_view(&self, name: &str) -> Option<Vec<Vec<Value>>> {
        let mvs = self.materialized_views.read().unwrap();
        mvs.get(name).map(|(_, data, _)| data.clone())
    }

    pub fn get_materialized_view_with_columns(
        &self,
        name: &str,
    ) -> Option<(Vec<Vec<Value>>, Vec<String>)> {
        let mvs = self.materialized_views.read().unwrap();
        mvs.get(name).map(|(_, data, columns)| (data.clone(), columns.clone()))
    }
}
