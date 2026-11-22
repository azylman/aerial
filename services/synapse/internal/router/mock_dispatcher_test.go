package router

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"

	"github.com/bwmarrin/discordgo"
)

type mockOutboundDispatcher struct {
	mu                           sync.Mutex
	reconcileCalls               []fluxKustomizationRef
	reconcileRequestedAts        []string
	promptCalls                  []PromptRequest
	dispatchCalls                []DispatchRequest
	directMessageCalls           []DirectMessageRequest
	discordMessageCalls          []DiscordMessageIngressRequest
	discordInteractionCalls      []*discordgo.Interaction
	discordInteractionAgentCalls []string
	discoverErr                  error
	reconcileErrs                map[string]error
	sourceRepo                   string
	sourceRepoErr                error
	promptErr                    error
	dispatchErr                  error
	directMessageErr             error
	discordMessageErr            error
	discordMessageFailCount      int
	discordMessageAttempts       int
	discordInteractionErr        error
	kustomizationsByImage        map[string][]fluxKustomizationRef
	workloads                    []workloadRollout
	workloadsErr                 error
	configMaps                   []configMapChange
	configMapsErr                error
	repoKustomizations           map[string][]fluxKustomizationRef
	repoDiscoverErr              error
}

func (m *mockOutboundDispatcher) DiscoverImageKustomizations(ctx context.Context, imageNames []string) ([]fluxKustomizationRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.discoverErr != nil {
		return nil, m.discoverErr
	}
	var refs []fluxKustomizationRef
	for _, name := range imageNames {
		for _, ref := range m.kustomizationsByImage[name] {
			if !slices.Contains(refs, ref) {
				refs = append(refs, ref)
			}
		}
	}
	return refs, nil
}

func (m *mockOutboundDispatcher) ResolveFluxSourceRepo(ctx context.Context, obj FluxInvolvedObject) (string, error) {
	return m.sourceRepo, m.sourceRepoErr
}

func (m *mockOutboundDispatcher) DiscoverRepoKustomizations(ctx context.Context, repo string) ([]fluxKustomizationRef, error) {
	if m.repoDiscoverErr != nil {
		return nil, m.repoDiscoverErr
	}
	return m.repoKustomizations[strings.TrimSpace(repo)], nil
}

func (m *mockOutboundDispatcher) DispatchPrompt(ctx context.Context, req PromptRequest) error {
	m.mu.Lock()
	m.promptCalls = append(m.promptCalls, req)
	m.mu.Unlock()
	return m.promptErr
}

func (m *mockOutboundDispatcher) DispatchBackground(ctx context.Context, req DispatchRequest) error {
	m.mu.Lock()
	m.dispatchCalls = append(m.dispatchCalls, req)
	m.mu.Unlock()
	return m.dispatchErr
}

func (m *mockOutboundDispatcher) DispatchDirectMessage(ctx context.Context, req DirectMessageRequest) error {
	m.mu.Lock()
	m.directMessageCalls = append(m.directMessageCalls, req)
	m.mu.Unlock()
	return m.directMessageErr
}

func (m *mockOutboundDispatcher) ReconcileCalls() []fluxKustomizationRef {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]fluxKustomizationRef(nil), m.reconcileCalls...)
}

func (m *mockOutboundDispatcher) PromptCalls() []PromptRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]PromptRequest(nil), m.promptCalls...)
}

func (m *mockOutboundDispatcher) DispatchCalls() []DispatchRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]DispatchRequest(nil), m.dispatchCalls...)
}

func (m *mockOutboundDispatcher) DirectMessageCalls() []DirectMessageRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]DirectMessageRequest(nil), m.directMessageCalls...)
}

func (m *mockOutboundDispatcher) DispatchDiscordMessage(ctx context.Context, req DiscordMessageIngressRequest) error {
	m.mu.Lock()
	m.discordMessageCalls = append(m.discordMessageCalls, req)
	m.discordMessageAttempts++
	if m.discordMessageFailCount > 0 {
		if m.discordMessageAttempts <= m.discordMessageFailCount {
			err := m.discordMessageErr
			if err == nil {
				err = errors.New("transient error")
			}
			m.mu.Unlock()
			return err
		}
		m.mu.Unlock()
		return nil
	}
	err := m.discordMessageErr
	m.mu.Unlock()
	return err
}

func (m *mockOutboundDispatcher) DispatchDiscordInteraction(ctx context.Context, interaction *discordgo.Interaction) error {
	m.mu.Lock()
	m.discordInteractionCalls = append(m.discordInteractionCalls, interaction)
	m.mu.Unlock()
	return m.discordInteractionErr
}

func (m *mockOutboundDispatcher) DispatchDiscordInteractionForAgent(ctx context.Context, agent string, interaction *discordgo.Interaction) error {
	m.mu.Lock()
	m.discordInteractionAgentCalls = append(m.discordInteractionAgentCalls, agent)
	m.discordInteractionCalls = append(m.discordInteractionCalls, interaction)
	m.mu.Unlock()
	return m.discordInteractionErr
}

func (m *mockOutboundDispatcher) DiscordInteractionAgentCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.discordInteractionAgentCalls...)
}

func (m *mockOutboundDispatcher) DiscordMessageCalls() []DiscordMessageIngressRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]DiscordMessageIngressRequest(nil), m.discordMessageCalls...)
}

func (m *mockOutboundDispatcher) DiscordInteractionCalls() []*discordgo.Interaction {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*discordgo.Interaction(nil), m.discordInteractionCalls...)
}
