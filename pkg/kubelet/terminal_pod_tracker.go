package kubelet

import (
	"sync"

	"k8s.io/apimachinery/pkg/types"
)

// terminalPodTracker coordinates the release of local tracking state for pods
// that have reached a terminal phase.
//
// A terminal pod must satisfy two independent conditions before the kubelet
// stops maintaining it:
//
//   - synced: the status manager has durably written the terminal status to the
//     API server. Releasing before this point could lose the final status
//     patch or leave the pod with a stale phase.
//   - released: the pod manager entry, allocation accounting, and (via the next
//     SyncKnownPods pass) the pod worker have been reclaimed.
//
// The status manager notifies this tracker when a terminal status is synced.
// HandlePodCleanups then releases a pod once its worker is also finished, which
// guarantees runtime events and the final status patch have both completed.
// The marker is local to this kubelet process and is dropped when the pod is
// removed from config, keeping the map bounded by the number of terminal pods
// currently visible in config.
type terminalPodTracker struct {
	lock     sync.Mutex
	synced   map[types.UID]struct{}
	released map[types.UID]struct{}
}

func newTerminalPodTracker() *terminalPodTracker {
	return &terminalPodTracker{
		synced:   make(map[types.UID]struct{}),
		released: make(map[types.UID]struct{}),
	}
}

func (t *terminalPodTracker) markSynced(uid types.UID) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.synced[uid] = struct{}{}
}

func (t *terminalPodTracker) isSynced(uid types.UID) bool {
	t.lock.Lock()
	defer t.lock.Unlock()
	_, ok := t.synced[uid]
	return ok
}

func (t *terminalPodTracker) markReleased(uid types.UID) {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.synced, uid)
	t.released[uid] = struct{}{}
}

func (t *terminalPodTracker) isReleased(uid types.UID) bool {
	t.lock.Lock()
	defer t.lock.Unlock()
	_, ok := t.released[uid]
	return ok
}

func (t *terminalPodTracker) clear(uid types.UID) {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.synced, uid)
	delete(t.released, uid)
}
