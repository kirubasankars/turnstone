#[cfg(unix)]
mod imp {
    use super::super::ErrUnsupported;
    use std::error::Error;
    use std::io;

    pub fn lock(b: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
        if b.is_empty() {
            return Ok(());
        }
        let rc = unsafe { libc::mlock(b.as_ptr() as *const _, b.len()) };
        if rc == 0 {
            Ok(())
        } else {
            Err(Box::new(io::Error::last_os_error()))
        }
    }

    pub fn unlock(b: &[u8]) {
        if b.is_empty() {
            return;
        }
        unsafe {
            libc::munlock(b.as_ptr() as *const _, b.len());
        }
    }

    pub fn supported() -> bool {
        true
    }

    pub fn is_denied(err: &(dyn Error + 'static)) -> bool {
        if err.downcast_ref::<ErrUnsupported>().is_some() {
            return true;
        }
        if let Some(io_err) = err.downcast_ref::<io::Error>() {
            return matches!(
                io_err.raw_os_error(),
                Some(libc::EPERM) | Some(libc::EACCES) | Some(libc::ENOMEM) | Some(libc::EAGAIN)
            );
        }
        false
    }
}

#[cfg(not(unix))]
mod imp {
    use super::super::ErrUnsupported;
    use std::error::Error;

    pub fn lock(_b: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
        Err(Box::new(ErrUnsupported))
    }

    pub fn unlock(_b: &[u8]) {}

    pub fn supported() -> bool {
        false
    }

    pub fn is_denied(err: &(dyn Error + 'static)) -> bool {
        err.downcast_ref::<ErrUnsupported>().is_some()
    }
}

pub use imp::*;
