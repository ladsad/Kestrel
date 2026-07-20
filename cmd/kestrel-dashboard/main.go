package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/ladsad/kestrel/pkg/resp"
)

var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FAFAFA")).
			Background(lipgloss.Color("#7D56F4")).
			Padding(0, 1).
			MarginBottom(1)
	
	nodeStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			Padding(1, 2)
			
	leaderStyle = nodeStyle.Copy().
			BorderForeground(lipgloss.Color("#04B575"))
			
	followerStyle = nodeStyle.Copy().
			BorderForeground(lipgloss.Color("#3C3C3C"))
			
	deadStyle = nodeStyle.Copy().
			BorderForeground(lipgloss.Color("#FF0000"))
			
	statusStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#04B575"))
	warnStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("#F2C94C"))
	errStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("#FF0000"))
)

type NodeState struct {
	Address        string
	Role           string
	Term           string
	LastLogIndex   string
	AppliedIndex   string
	IsAlive        bool
	Latency        time.Duration
}

type model struct {
	peers       []string
	nodes       map[string]NodeState
	lastUpdated time.Time
}

type tickMsg time.Time
type nodeResultMsg struct {
	addr  string
	state NodeState
}

func initialModel(peers []string) model {
	m := model{
		peers: peers,
		nodes: make(map[string]NodeState),
	}
	for _, p := range peers {
		m.nodes[p] = NodeState{Address: p, IsAlive: false}
	}
	return m
}

func (m model) Init() tea.Cmd {
	return tickCmd()
}

func tickCmd() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func fetchNodeState(addr string) tea.Cmd {
	return func() tea.Msg {
		start := time.Now()
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err != nil {
			return nodeResultMsg{addr: addr, state: NodeState{Address: addr, IsAlive: false, Latency: time.Since(start)}}
		}
		defer conn.Close()

		conn.SetDeadline(time.Now().Add(500 * time.Millisecond))
		w := resp.NewWriter(conn)
		r := resp.NewReader(conn)

		err = w.Write(resp.NewArray([]resp.Value{resp.NewBulkString([]byte("INFO")), resp.NewBulkString([]byte("REPLICATION"))}))
		if err != nil {
			return nodeResultMsg{addr: addr, state: NodeState{Address: addr, IsAlive: false, Latency: time.Since(start)}}
		}

		val, err := r.Read()
		if err != nil || val.Type != resp.TypeBulkString {
			return nodeResultMsg{addr: addr, state: NodeState{Address: addr, IsAlive: false, Latency: time.Since(start)}}
		}

		lines := strings.Split(string(val.Bulk), "\r\n")
		state := NodeState{Address: addr, IsAlive: true, Latency: time.Since(start)}
		
		for _, line := range lines {
			parts := strings.Split(line, ":")
			if len(parts) == 2 {
				switch parts[0] {
				case "role":
					state.Role = parts[1]
				case "term":
					state.Term = parts[1]
				case "last_log_index":
					state.LastLogIndex = parts[1]
				case "applied_index":
					state.AppliedIndex = parts[1]
				}
			}
		}

		return nodeResultMsg{addr: addr, state: state}
	}
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.String() == "q" || msg.String() == "ctrl+c" {
			return m, tea.Quit
		}
	case tickMsg:
		var cmds []tea.Cmd
		for _, peer := range m.peers {
			cmds = append(cmds, fetchNodeState(peer))
		}
		cmds = append(cmds, tickCmd())
		return m, tea.Batch(cmds...)
	case nodeResultMsg:
		m.nodes[msg.addr] = msg.state
		m.lastUpdated = time.Now()
	}
	return m, nil
}

func (m model) View() string {
	s := titleStyle.Render(" Kestrel Live Dashboard ") + "\n\n"
	
	s += fmt.Sprintf("Last Updated: %s (Press 'q' to quit)\n\n", m.lastUpdated.Format("15:04:05"))

	var views []string
	
	for _, peer := range m.peers {
		state := m.nodes[peer]
		
		var nodeStr string
		nodeStr += fmt.Sprintf("Node: %s\n", state.Address)
		
		if !state.IsAlive {
			nodeStr += errStyle.Render("Status: DEAD") + "\n"
			nodeStr += "Latency: N/A\n"
			views = append(views, deadStyle.Render(nodeStr))
			continue
		}

		if state.Role == "Leader" {
			nodeStr += statusStyle.Render(fmt.Sprintf("Role: %s", state.Role)) + "\n"
		} else if state.Role == "Follower" {
			nodeStr += fmt.Sprintf("Role: %s\n", state.Role)
		} else {
			nodeStr += warnStyle.Render(fmt.Sprintf("Role: %s", state.Role)) + "\n" // Candidate
		}
		
		nodeStr += fmt.Sprintf("Term: %s\n", state.Term)
		nodeStr += fmt.Sprintf("Log Index: %s\n", state.LastLogIndex)
		nodeStr += fmt.Sprintf("Applied: %s\n", state.AppliedIndex)
		nodeStr += fmt.Sprintf("Latency: %v\n", state.Latency)

		if state.Role == "Leader" {
			views = append(views, leaderStyle.Render(nodeStr))
		} else {
			views = append(views, followerStyle.Render(nodeStr))
		}
	}
	var rows []string
	for i := 0; i < len(views); i += 4 {
		end := i + 4
		if end > len(views) {
			end = len(views)
		}
		rows = append(rows, lipgloss.JoinHorizontal(lipgloss.Top, views[i:end]...))
	}

	return s + lipgloss.JoinVertical(lipgloss.Left, rows...) + "\n"
}

func main() {
	peersFlag := flag.String("peers", "127.0.0.1:6380,127.0.0.1:6381,127.0.0.1:6382", "Comma-separated list of peer addresses")
	flag.Parse()

	peers := strings.Split(*peersFlag, ",")

	p := tea.NewProgram(initialModel(peers))
	if _, err := p.Run(); err != nil {
		log.Fatalf("Alas, there's been an error: %v", err)
	}
}
