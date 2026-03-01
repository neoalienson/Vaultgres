use super::{Function, TableSchema, Tuple, Value};
use crate::parser::ast::{ColumnDef, CreateIndexStmt, CreateTriggerStmt, DataType, SelectStmt};
use crate::transaction::{TransactionManager, TupleHeader};
use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

pub struct Persistence;

impl Persistence {
    pub fn save(
        data_dir: &str,
        tables: &HashMap<String, TableSchema>,
        data: &HashMap<String, Vec<Tuple>>,
    ) -> Result<(), String> {
        create_dir_all(data_dir).map_err(|e| format!("Failed to create data dir: {}", e))?;

        let catalog_path = format!("{}/catalog.bin", data_dir);
        let file = File::create(&catalog_path)
            .map_err(|e| format!("Failed to create catalog file: {}", e))?;
        let mut writer = BufWriter::new(file);

        write_u32(&mut writer, tables.len() as u32)?;

        for (table_name, schema) in tables.iter() {
            write_string(&mut writer, table_name)?;
            write_u32(&mut writer, schema.columns.len() as u32)?;

            for col in &schema.columns {
                write_string(&mut writer, &col.name)?;
                write_data_type(&mut writer, &col.data_type)?;
            }

            let tuples = data.get(table_name).map(|t| t.as_slice()).unwrap_or(&[]);
            write_u32(&mut writer, tuples.len() as u32)?;

            for tuple in tuples {
                write_u32(&mut writer, tuple.data.len() as u32)?;
                for value in &tuple.data {
                    write_value(&mut writer, value)?;
                }
            }
        }

        writer.flush().map_err(|e| format!("Failed to flush: {}", e))?;
        log::info!("💾 Saved {} tables to {}", tables.len(), catalog_path);
        Ok(())
    }

    pub fn save_views(data_dir: &str, views: &HashMap<String, SelectStmt>) -> Result<(), String> {
        let path = format!("{}/views.json", data_dir);
        let json = serde_json::to_string(views)
            .map_err(|e| format!("Failed to serialize views: {}", e))?;
        std::fs::write(&path, json).map_err(|e| format!("Failed to write views: {}", e))?;
        log::info!("💾 Saved {} views", views.len());
        Ok(())
    }

    pub fn load_views(data_dir: &str) -> Result<HashMap<String, SelectStmt>, String> {
        let path = format!("{}/views.json", data_dir);
        if !Path::new(&path).exists() {
            return Ok(HashMap::new());
        }
        let json =
            std::fs::read_to_string(&path).map_err(|e| format!("Failed to read views: {}", e))?;
        serde_json::from_str(&json).map_err(|e| format!("Failed to deserialize views: {}", e))
    }

    pub fn save_triggers(
        data_dir: &str,
        triggers: &HashMap<String, CreateTriggerStmt>,
    ) -> Result<(), String> {
        let path = format!("{}/triggers.json", data_dir);
        let json = serde_json::to_string(triggers)
            .map_err(|e| format!("Failed to serialize triggers: {}", e))?;
        std::fs::write(&path, json).map_err(|e| format!("Failed to write triggers: {}", e))?;
        log::info!("💾 Saved {} triggers", triggers.len());
        Ok(())
    }

    pub fn load_triggers(data_dir: &str) -> Result<HashMap<String, CreateTriggerStmt>, String> {
        let path = format!("{}/triggers.json", data_dir);
        if !Path::new(&path).exists() {
            return Ok(HashMap::new());
        }
        let json = std::fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read triggers: {}", e))?;
        serde_json::from_str(&json).map_err(|e| format!("Failed to deserialize triggers: {}", e))
    }

    pub fn save_indexes(
        data_dir: &str,
        indexes: &HashMap<String, CreateIndexStmt>,
    ) -> Result<(), String> {
        let path = format!("{}/indexes.json", data_dir);
        let json = serde_json::to_string(indexes)
            .map_err(|e| format!("Failed to serialize indexes: {}", e))?;
        std::fs::write(&path, json).map_err(|e| format!("Failed to write indexes: {}", e))?;
        log::info!("💾 Saved {} indexes", indexes.len());
        Ok(())
    }

    pub fn load_indexes(data_dir: &str) -> Result<HashMap<String, CreateIndexStmt>, String> {
        let path = format!("{}/indexes.json", data_dir);
        if !Path::new(&path).exists() {
            return Ok(HashMap::new());
        }
        let json =
            std::fs::read_to_string(&path).map_err(|e| format!("Failed to read indexes: {}", e))?;
        serde_json::from_str(&json).map_err(|e| format!("Failed to deserialize indexes: {}", e))
    }

    pub fn save_functions(
        data_dir: &str,
        functions: &HashMap<String, Vec<Function>>,
    ) -> Result<(), String> {
        let path = format!("{}/functions.json", data_dir);
        let json = serde_json::to_string(functions)
            .map_err(|e| format!("Failed to serialize functions: {}", e))?;
        std::fs::write(&path, json).map_err(|e| format!("Failed to write functions: {}", e))?;
        log::info!("💾 Saved {} function groups", functions.len());
        Ok(())
    }

    pub fn load_functions(data_dir: &str) -> Result<HashMap<String, Vec<Function>>, String> {
        let path = format!("{}/functions.json", data_dir);
        if !Path::new(&path).exists() {
            return Ok(HashMap::new());
        }
        let json = std::fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read functions: {}", e))?;
        serde_json::from_str(&json).map_err(|e| format!("Failed to deserialize functions: {}", e))
    }

    pub fn load(
        data_dir: &str,
        tables: &mut HashMap<String, TableSchema>,
        data: &mut HashMap<String, Vec<Tuple>>,
        txn_mgr: &Arc<TransactionManager>,
    ) -> Result<(), String> {
        let catalog_path = format!("{}/catalog.bin", data_dir);

        if !Path::new(&catalog_path).exists() {
            log::info!("📂 No existing catalog found, starting fresh");
            return Ok(());
        }

        let file =
            File::open(&catalog_path).map_err(|e| format!("Failed to open catalog file: {}", e))?;
        let mut reader = BufReader::new(file);

        let num_tables = read_u32(&mut reader)?;

        for _ in 0..num_tables {
            let table_name = read_string(&mut reader)?;
            let num_columns = read_u32(&mut reader)?;
            let mut columns = Vec::new();

            for _ in 0..num_columns {
                let col_name = read_string(&mut reader)?;
                let data_type = read_data_type(&mut reader)?;
                columns.push(ColumnDef { name: col_name, data_type });
            }

            let schema = TableSchema { name: table_name.clone(), columns };
            let num_tuples = read_u32(&mut reader)?;
            let mut tuples = Vec::new();

            for _ in 0..num_tuples {
                let num_values = read_u32(&mut reader)?;
                let mut values = Vec::new();

                for _ in 0..num_values {
                    values.push(read_value(&mut reader)?);
                }

                let txn = txn_mgr.begin();
                let header = TupleHeader::new(txn.xid);
                txn_mgr.commit(txn.xid).map_err(|e| e.to_string())?;

                tuples.push(Tuple { header, data: values });
            }

            tables.insert(table_name.clone(), schema);
            data.insert(table_name, tuples);
        }

        log::info!("📂 Loaded {} tables from {}", num_tables, catalog_path);
        Ok(())
    }
}

fn write_u32<W: Write>(writer: &mut W, value: u32) -> Result<(), String> {
    writer.write_all(&value.to_le_bytes()).map_err(|e| format!("Write error: {}", e))
}

fn read_u32<R: Read>(reader: &mut R) -> Result<u32, String> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).map_err(|e| format!("Read error: {}", e))?;
    Ok(u32::from_le_bytes(buf))
}

fn write_string<W: Write>(writer: &mut W, s: &str) -> Result<(), String> {
    write_u32(writer, s.len() as u32)?;
    writer.write_all(s.as_bytes()).map_err(|e| format!("Write error: {}", e))
}

fn read_string<R: Read>(reader: &mut R) -> Result<String, String> {
    let len = read_u32(reader)?;
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf).map_err(|e| format!("Read error: {}", e))?;
    String::from_utf8(buf).map_err(|e| format!("UTF-8 error: {}", e))
}

fn write_data_type<W: Write>(writer: &mut W, dt: &DataType) -> Result<(), String> {
    match dt {
        DataType::Int => writer.write_all(&[0]).map_err(|e| format!("Write error: {}", e))?,
        DataType::Text => writer.write_all(&[1]).map_err(|e| format!("Write error: {}", e))?,
        DataType::Varchar(len) => {
            writer.write_all(&[2]).map_err(|e| format!("Write error: {}", e))?;
            write_u32(writer, *len)?;
        }
    }
    Ok(())
}

fn read_data_type<R: Read>(reader: &mut R) -> Result<DataType, String> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf).map_err(|e| format!("Read error: {}", e))?;

    match buf[0] {
        0 => Ok(DataType::Int),
        1 => Ok(DataType::Text),
        2 => {
            let len = read_u32(reader)?;
            Ok(DataType::Varchar(len))
        }
        _ => Err(format!("Unknown data type: {}", buf[0])),
    }
}

fn write_value<W: Write>(writer: &mut W, value: &Value) -> Result<(), String> {
    match value {
        Value::Int(n) => {
            writer.write_all(&[0]).map_err(|e| format!("Write error: {}", e))?;
            writer.write_all(&n.to_le_bytes()).map_err(|e| format!("Write error: {}", e))?;
        }
        Value::Float(f) => {
            writer.write_all(&[3]).map_err(|e| format!("Write error: {}", e))?;
            writer.write_all(&f.to_le_bytes()).map_err(|e| format!("Write error: {}", e))?;
        }
        Value::Bool(b) => {
            writer.write_all(&[4]).map_err(|e| format!("Write error: {}", e))?;
            writer
                .write_all(&[if *b { 1 } else { 0 }])
                .map_err(|e| format!("Write error: {}", e))?;
        }
        Value::Text(s) => {
            writer.write_all(&[1]).map_err(|e| format!("Write error: {}", e))?;
            write_string(writer, s)?;
        }
        Value::Array(arr) => {
            writer.write_all(&[5]).map_err(|e| format!("Write error: {}", e))?;
            write_u32(writer, arr.len() as u32)?;
            for v in arr {
                write_value(writer, v)?;
            }
        }
        Value::Json(j) => {
            writer.write_all(&[6]).map_err(|e| format!("Write error: {}", e))?;
            write_string(writer, j)?;
        }
        Value::Null => {
            writer.write_all(&[2]).map_err(|e| format!("Write error: {}", e))?;
        }
    }
    Ok(())
}

fn read_value<R: Read>(reader: &mut R) -> Result<Value, String> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf).map_err(|e| format!("Read error: {}", e))?;

    match buf[0] {
        0 => {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf).map_err(|e| format!("Read error: {}", e))?;
            Ok(Value::Int(i64::from_le_bytes(buf)))
        }
        1 => {
            let s = read_string(reader)?;
            Ok(Value::Text(s))
        }
        2 => Ok(Value::Null),
        3 => {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf).map_err(|e| format!("Read error: {}", e))?;
            Ok(Value::Float(f64::from_le_bytes(buf)))
        }
        4 => {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf).map_err(|e| format!("Read error: {}", e))?;
            Ok(Value::Bool(buf[0] != 0))
        }
        5 => {
            let len = read_u32(reader)?;
            let mut arr = Vec::new();
            for _ in 0..len {
                arr.push(read_value(reader)?);
            }
            Ok(Value::Array(arr))
        }
        6 => {
            let s = read_string(reader)?;
            Ok(Value::Json(s))
        }
        _ => Err(format!("Unknown value type: {}", buf[0])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::ColumnDef;
    use crate::transaction::TupleHeader;
    use std::fs;

    #[test]
    fn test_save_and_load() {
        let test_dir = "/tmp/rustgres_test_persistence";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        let mut tables = HashMap::new();
        let mut data = HashMap::new();

        let schema = TableSchema {
            name: "users".to_string(),
            columns: vec![
                ColumnDef { name: "id".to_string(), data_type: DataType::Int },
                ColumnDef { name: "name".to_string(), data_type: DataType::Text },
            ],
        };

        let txn_mgr = Arc::new(TransactionManager::new());
        let txn = txn_mgr.begin();
        let header = TupleHeader::new(txn.xid);
        txn_mgr.commit(txn.xid).unwrap();

        let tuples = vec![
            Tuple { header: header, data: vec![Value::Int(1), Value::Text("Alice".to_string())] },
            Tuple { header: header, data: vec![Value::Int(2), Value::Text("Bob".to_string())] },
        ];

        tables.insert("users".to_string(), schema);
        data.insert("users".to_string(), tuples);

        Persistence::save(test_dir, &tables, &data).unwrap();

        let mut loaded_tables = HashMap::new();
        let mut loaded_data = HashMap::new();
        Persistence::load(test_dir, &mut loaded_tables, &mut loaded_data, &txn_mgr).unwrap();

        assert_eq!(loaded_tables.len(), 1);
        assert_eq!(loaded_data.get("users").unwrap().len(), 2);

        fs::remove_dir_all(test_dir).unwrap();
    }

    #[test]
    fn test_save_with_null_values() {
        let test_dir = "/tmp/rustgres_test_null";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        let mut tables = HashMap::new();
        let mut data = HashMap::new();

        let schema = TableSchema {
            name: "test".to_string(),
            columns: vec![
                ColumnDef { name: "id".to_string(), data_type: DataType::Int },
                ColumnDef { name: "value".to_string(), data_type: DataType::Int },
            ],
        };

        let txn_mgr = Arc::new(TransactionManager::new());
        let txn = txn_mgr.begin();
        let header = TupleHeader::new(txn.xid);
        txn_mgr.commit(txn.xid).unwrap();

        let tuples = vec![Tuple { header: header, data: vec![Value::Int(1), Value::Null] }];

        tables.insert("test".to_string(), schema);
        data.insert("test".to_string(), tuples);

        Persistence::save(test_dir, &tables, &data).unwrap();

        let mut loaded_tables = HashMap::new();
        let mut loaded_data = HashMap::new();
        Persistence::load(test_dir, &mut loaded_tables, &mut loaded_data, &txn_mgr).unwrap();

        assert_eq!(loaded_data.get("test").unwrap()[0].data[1], Value::Null);

        fs::remove_dir_all(test_dir).unwrap();
    }

    #[test]
    fn test_load_nonexistent_catalog() {
        let test_dir = "/tmp/rustgres_test_nonexistent";
        let _ = fs::remove_dir_all(test_dir);

        let mut tables = HashMap::new();
        let mut data = HashMap::new();
        let txn_mgr = Arc::new(TransactionManager::new());

        let result = Persistence::load(test_dir, &mut tables, &mut data, &txn_mgr);
        assert!(result.is_ok());
        assert_eq!(tables.len(), 0);
    }
}
