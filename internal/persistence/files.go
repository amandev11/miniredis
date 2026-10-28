package persistence

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
)

// getUint32 and putUint32 keep the little-endian choice in one place.
func getUint32(b []byte) uint32    { return binary.LittleEndian.Uint32(b) }
func putUint32(b []byte, v uint32) { binary.LittleEndian.PutUint32(b, v) }

func crc32Checksum(b []byte) uint32 { return crc32.Checksum(b, crcTable) }

// ensureDirectory creates dir with owner-only permissions when it is missing.
//
// 0700 keeps the journal and snapshots readable only by the account running
// the server. Stored values never leave the process through these files, so
// this matters: an unlocked data directory would expose every key.
func ensureDirectory(dir string) error {
	if dir == "" || dir == "." {
		return nil
	}
	fi, err := os.Lstat(dir)
	switch {
	case err == nil:
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("data directory %s is a symbolic link; refusing to use it", dir)
		}
		if !fi.IsDir() {
			return fmt.Errorf("data path %s exists and is not a directory", dir)
		}
		return nil
	case os.IsNotExist(err):
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create data directory %s: %w", dir, err)
		}
		return nil
	default:
		return fmt.Errorf("stat data directory %s: %w", dir, err)
	}
}

// ensureRegularFile reports whether path exists and refuses to follow symbolic
// links or to use a path that is not a regular file. exist is true when a
// usable file is already present.
func ensureRegularFile(path string) (exist bool, err error) {
	fi, err := os.Lstat(path)
	switch {
	case err == nil:
		if fi.Mode()&os.ModeSymlink != 0 {
			return false, fmt.Errorf("%s is a symbolic link; refusing to use it for persistence", path)
		}
		if !fi.Mode().IsRegular() {
			return false, fmt.Errorf("%s exists but is not a regular file", path)
		}
		return true, nil
	case os.IsNotExist(err):
		return false, nil
	default:
		return false, fmt.Errorf("stat %s: %w", path, err)
	}
}

// syncDir flushes a directory entry so that a rename or truncation survives a
// power loss. Some filesystems do not support fsync on a directory; those
// return nil rather than a spurious error.
func syncDir(dir string) error {
	if dir == "" {
		dir = "."
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open directory %s for sync: %w", dir, err)
	}
	defer d.Close()

	if err := d.Sync(); err != nil {
		// EINVAL/ENOTSUP are reported by filesystems that cannot sync a
		// directory handle; that is a platform limitation, not a failure of
		// this write. Any other error is a real I/O problem.
		if isUnsupportedSync(err) {
			return nil
		}
		return fmt.Errorf("fsync directory %s: %w", dir, err)
	}
	return nil
}

// isUnsupportedSync reports whether an fsync error means "not supported".
func isUnsupportedSync(err error) bool {
	pathErr, ok := err.(*fs.PathError)
	if !ok {
		return false
	}
	switch pathErr.Err.Error() {
	case "invalid argument", "operation not supported", "not supported":
		return true
	default:
		return false
	}
}

// writeFileAtomic writes data to a temporary file in the same directory,
// flushes it, and renames it over destination.
//
// The sequence is the standard crash-safe replace: the previous contents of
// destination stay intact until the rename, so a crash at any point leaves
// either the old file or the new one, never a half-written mixture.
func writeFileAtomic(destination string, write func(*os.File) error) (err error) {
	dir := filepath.Dir(destination)
	if err := ensureDirectory(dir); err != nil {
		return err
	}
	tmp := destination + ".tmp"
	// Remove a leftover temporary file from an interrupted run. It is never a
	// source of truth; the real file is only ever replaced by rename.
	if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale temporary file %s: %w", tmp, err)
	}

	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create temporary file %s: %w", tmp, err)
	}
	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(tmp)
	}

	if err := write(f); err != nil {
		cleanup()
		return err
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("fsync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, destination); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s to %s: %w", tmp, destination, err)
	}
	return syncDir(dir)
}
