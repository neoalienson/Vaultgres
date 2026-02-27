use std::io;
use std::path::Path;

/// Filesystem abstraction for testing
pub trait FileSystem: Send + Sync {
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    fn read(&self, path: &Path) -> io::Result<Vec<u8>>;
    fn write(&self, path: &Path, data: &[u8]) -> io::Result<()>;
    fn exists(&self, path: &Path) -> bool;
    fn remove_file(&self, path: &Path) -> io::Result<()>;
}

/// Real filesystem implementation
pub struct RealFileSystem;

impl FileSystem for RealFileSystem {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        std::fs::read(path)
    }

    fn write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        std::fs::write(path, data)
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }
}

#[cfg(test)]
pub mod mock {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Mock filesystem for testing
    pub struct MockFileSystem {
        files: Mutex<HashMap<String, Vec<u8>>>,
        dirs: Mutex<Vec<String>>,
    }

    impl MockFileSystem {
        pub fn new() -> Self {
            Self {
                files: Mutex::new(HashMap::new()),
                dirs: Mutex::new(Vec::new()),
            }
        }

        pub fn file_count(&self) -> usize {
            self.files.lock().unwrap().len()
        }

        pub fn dir_count(&self) -> usize {
            self.dirs.lock().unwrap().len()
        }
    }

    impl FileSystem for MockFileSystem {
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            self.dirs.lock().unwrap().push(path.to_string_lossy().to_string());
            Ok(())
        }

        fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
            let files = self.files.lock().unwrap();
            files.get(path.to_str().unwrap())
                .cloned()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "File not found"))
        }

        fn write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
            self.files.lock().unwrap().insert(
                path.to_string_lossy().to_string(),
                data.to_vec()
            );
            Ok(())
        }

        fn exists(&self, path: &Path) -> bool {
            let path_str = path.to_string_lossy().to_string();
            self.files.lock().unwrap().contains_key(&path_str) ||
            self.dirs.lock().unwrap().contains(&path_str)
        }

        fn remove_file(&self, path: &Path) -> io::Result<()> {
            self.files.lock().unwrap().remove(path.to_str().unwrap());
            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_mock_filesystem() {
            let fs = MockFileSystem::new();
            
            // Create directory
            fs.create_dir_all(Path::new("/test/dir")).unwrap();
            assert_eq!(fs.dir_count(), 1);
            
            // Write file
            fs.write(Path::new("/test/file.txt"), b"hello").unwrap();
            assert_eq!(fs.file_count(), 1);
            
            // Read file
            let data = fs.read(Path::new("/test/file.txt")).unwrap();
            assert_eq!(data, b"hello");
            
            // Check exists
            assert!(fs.exists(Path::new("/test/file.txt")));
            assert!(!fs.exists(Path::new("/nonexistent")));
        }
    }
}
