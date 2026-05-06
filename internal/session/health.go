package session

func (m *Manager) setStateLocked(state ConnectionState) {
	m.health.State = state
	if m.hooks.Metrics != nil {
		m.hooks.Metrics.SetConnectionState(string(state))
	}
}

func (m *Manager) setConnectedLocked(connectedURL string, jsReady, kvReady bool) {
	m.health.ConnectedURL = connectedURL
	m.health.JetStreamReady = jsReady
	m.health.KVReady = kvReady
	m.health.LastError = ""
	if jsReady && kvReady {
		m.setStateLocked(StateConnected)
		return
	}
	m.setStateLocked(StateDegraded)
}

func (m *Manager) setReconnectingLocked(lastError error) {
	m.health.JetStreamReady = false
	m.health.KVReady = false
	if lastError != nil {
		m.health.LastError = lastError.Error()
	}
	m.setStateLocked(StateReconnecting)
}

func (m *Manager) setDegradedLocked(lastError error) {
	m.health.JetStreamReady = false
	m.health.KVReady = false
	if lastError != nil {
		m.health.LastError = lastError.Error()
	}
	m.setStateLocked(StateDegraded)
}

func (m *Manager) setClosedLocked(lastError error) {
	m.health.ConnectedURL = ""
	m.health.JetStreamReady = false
	m.health.KVReady = false
	if lastError != nil {
		m.health.LastError = lastError.Error()
	}
	m.setStateLocked(StateClosed)
}
