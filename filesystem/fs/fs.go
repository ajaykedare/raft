package fs

import (
	_ "fmt"
	"sync"
	"time"
)

type FileInfo struct {
	filename   string
	contents   []byte
	version    int
	absexptime time.Time
	timer      *time.Timer
}

type FS struct {
	sync.RWMutex
	Dir map[string]*FileInfo
	Gversion int
}

//var fs = &FS{dir: make(map[string]*FileInfo, 1000)}
//var gversion = 0 // global version

func (fi *FileInfo) cancelTimer() {
	if fi.timer != nil {
		fi.timer.Stop()
		fi.timer = nil
	}
}

func (fs *FS)ProcessMsg(msg *Msg) *Msg {
	switch msg.Kind {
	case 'r':
		return fs.processRead(msg)
	case 'w':
		return fs.processWrite(msg)
	case 'c':
		return fs.processCas(msg)
	case 'd':
		return fs.processDelete(msg)
	}

	// Default: Internal error. Shouldn't come here since
	// the msg should have been validated earlier.
	return &Msg{Kind: 'I'}
}

func (fs *FS) processRead(msg *Msg) *Msg {
	fs.RLock()
	defer fs.RUnlock()
	if fi := fs.Dir[msg.Filename]; fi != nil {
		remainingTime := 0
		if fi.timer != nil {
			remainingTime := int(fi.absexptime.Sub(time.Now()))
			if remainingTime < 0 {
				remainingTime = 0
			}
		}
		return &Msg{
			Kind:     'C',
			Filename: fi.filename,
			Contents: fi.contents,
			Numbytes: len(fi.contents),
			Exptime:  remainingTime,
			Version:  fi.version,
		}
	} else {
		return &Msg{Kind: 'F'} // file not found
	}
}

func (fs *FS)internalWrite(msg *Msg) *Msg {
	fi := fs.Dir[msg.Filename]
	if fi != nil {
		fi.cancelTimer()
	} else {
		fi = &FileInfo{}
	}

	fs.Gversion += 1
	fi.filename = msg.Filename
	fi.contents = msg.Contents
	fi.version = fs.Gversion

	var absexptime time.Time
	if msg.Exptime > 0 {
		dur := time.Duration(msg.Exptime) * time.Second
		absexptime = time.Now().Add(dur)
		timerFunc := func(name string, ver int) func() {
			return func() {
				fs.processDelete(&Msg{Kind: 'D',
					Filename: name,
					Version:  ver})
			}
		}(msg.Filename, fs.Gversion)

		fi.timer = time.AfterFunc(dur, timerFunc)
	}
	fi.absexptime = absexptime
	fs.Dir[msg.Filename] = fi

	return ok(fs.Gversion)
}

func (fs *FS) processWrite(msg *Msg) *Msg {
	fs.Lock()
	defer fs.Unlock()
	return fs.internalWrite(msg)
}

func (fs *FS)processCas(msg *Msg) *Msg {
	fs.Lock()
	defer fs.Unlock()

	if fi := fs.Dir[msg.Filename]; fi != nil {
		if msg.Version != fi.version {
			return &Msg{Kind: 'V', Version: fi.version}
		}
	}
	return fs.internalWrite(msg)
}

func (fs *FS)processDelete(msg *Msg) *Msg {
	fs.Lock()
	defer fs.Unlock()
	fi := fs.Dir[msg.Filename]
	if fi != nil {
		if msg.Version > 0 && fi.version != msg.Version {
			// non-zero msg.Version indicates a delete due to an expired timer
			return nil // nothing to do
		}
		fi.cancelTimer()
		delete(fs.Dir, msg.Filename)
		return ok(0)
	} else {
		return &Msg{Kind: 'F'} // file not found
	}

}

func ok(version int) *Msg {
	return &Msg{Kind: 'O', Version: version}
}
