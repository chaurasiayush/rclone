package policy

import (
	"context"
	"math"

	"github.com/rclone/rclone/backend/union/upstream"
	"github.com/rclone/rclone/fs"
)

func init() {
	registerPolicy("efs", &Efs{})
}

// Lfs stands for least free space
// Search category: same as eplfs.
// Action category: same as eplfs.
// Create category: Pick the drive with the least free space.
type Efs struct {
	EpLfs
}

// enough free space
func (p *EpLfs) efs(upstreams []*upstream.Fs, requiredSize any) (*upstream.Fs, error) {
	var minFreeSpace int64 = math.MaxInt64
	var efsupstream *upstream.Fs
	for _, u := range upstreams {
		space, err := u.GetFreeSpace()
		if err != nil {
			fs.LogPrintf(fs.LogLevelNotice, nil,
				"Free Space is not supported for upstream %s, treating as infinite", u.Name())
		}
		rsize := int64(u.Opt.MinFreeSpace)
		if requiredSize != nil {
			rsize = requiredSize.(int64)
		}
		if space < minFreeSpace && space > rsize {
			minFreeSpace = space
			efsupstream = u
		}
	}
	fs.LogPrintf(fs.LogLevelNotice, nil,
		"Uploading to upstream %s", efsupstream.Name())
	if efsupstream == nil {
		return nil, errNoUpstreamsFound
	}
	return efsupstream, nil
}

// Create category policy, governing the creation of files and directories
func (p *Efs) Create(ctx context.Context, upstreams []*upstream.Fs, path string) ([]*upstream.Fs, error) {
	fs.LogPrintf(fs.LogLevelNotice, nil,
		"POLICY: Using EFS policy")
	if len(upstreams) == 0 {
		return nil, fs.ErrorObjectNotFound
	}
	upstreams = filterNC(upstreams)
	if len(upstreams) == 0 {
		return nil, fs.ErrorPermissionDenied
	}
	requiredSize := ctx.Value("fileSize")
	// if requiredSize == nil {
	// 	return nil, fs.ErrorNotAFile
	// }
	u, err := p.efs(upstreams, requiredSize)
	return []*upstream.Fs{u}, err
}
