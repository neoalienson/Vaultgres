use crate::parser::ast::{ColumnDef, DataType};
use crate::transaction::{TransactionManager, TupleHeader};
use super::{Value, TableSchema, Tuple};
use std::collections::HashMap;
use std::fs::{File, create_dir_all};
use std::io::{Write, Read, BufWriter, BufReader};
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
        
        let file = File::open(&catalog_path)
            .map_err(|e| format!("Failed to open catalog file: {}", e))?;
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
    writer.write_all(&value.to_le_bytes())
        .map_err(|e| format!("Write error: {}", e))
}

fn read_u32<R: Read>(reader: &mut R) -> Result<u32, String> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)
        .map_err(|e| format!("Read error: {}", e))?;
    Ok(u32::from_le_bytes(buf))
}

fn write_string<W: Write>(writer: &mut W, s: &str) -> Result<(), String> {
    write_u32(writer, s.len() as u32)?;
    writer.write_all(s.as_bytes())
        .map_err(|e| format!("Write error: {}", e))
}

fn read_string<R: Read>(reader: &mut R) -> Result<String, String> {
    let len = read_u32(reader)?;
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf)
        .map_err(|e| format!("Read error: {}", e))?;
    String::from_utf8(buf)
        .map_err(|e| format!("UTF-8 error: {}", e))
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
    reader.read_exact(&mut buf)
        .map_err(|e| format!("Read error: {}", e))?;
    
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
            writer.write_all(&n.to_le_bytes())
                .map_err(|e| format!("Write error: {}", e))?;
        }
        Value::Text(s) => {
            writer.write_all(&[1]).map_err(|e| format!("Write error: {}", e))?;
            write_string(writer, s)?;
        }
        Value::Null => {
            writer.write_all(&[2]).map_err(|e| format!("Write error: {}", e))?;
        }
    }
    Ok(())
}

fn read_value<R: Read>(reader: &mut R) -> Result<Value, String> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)
        .map_err(|e| format!("Read error: {}", e))?;
    
    match buf[0] {
        0 => {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)
                .map_err(|e| format!("Read error: {}", e))?;
            Ok(Value::Int(i64::from_le_bytes(buf)))
        }
        1 => {
            let s = read_string(reader)?;
            Ok(Value::Text(s))
        }
        2 => Ok(Value::Null),
        _ => Err(format!("Unknown value type: {}", buf[0])),
    }
}
