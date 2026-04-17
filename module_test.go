package kubernetes

import (
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func TestUnmarshalCaddyfile(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		wantService      string
		wantNamespace    string
		wantPort         string
		wantMaxStaleness caddy.Duration
		wantErr          bool
	}{
		{
			name:        "simple service",
			input:       `dynamic kubernetes my-service`,
			wantService: "my-service",
		},
		{
			name:        "service and port shortcut",
			input:       `dynamic kubernetes my-service:8080`,
			wantService: "my-service",
			wantPort:    "8080",
		},
		{
			name:          "namespace and service shortcut",
			input:         `dynamic kubernetes my-ns/my-service`,
			wantNamespace: "my-ns",
			wantService:   "my-service",
		},
		{
			name:        "service and named port shortcut",
			input:       `dynamic kubernetes my-service:http`,
			wantService: "my-service",
			wantPort:    "http",
		},
		{
			name:          "namespace, service and port shortcut",
			input:         `dynamic kubernetes my-ns/my-service:8080`,
			wantNamespace: "my-ns",
			wantService:   "my-service",
			wantPort:      "8080",
		},
		{
			name:          "namespace, service and named port shortcut",
			input:         `dynamic kubernetes my-ns/my-service:http`,
			wantNamespace: "my-ns",
			wantService:   "my-service",
			wantPort:      "http",
		},
		{
			name: "full block",
			input: `dynamic kubernetes my-service {
				namespace other-ns
				port 8080
				allow_unready true
				allow_terminating true
				max_staleness 2m
			}`,
			wantService:      "my-service",
			wantNamespace:    "other-ns",
			wantPort:         "8080",
			wantMaxStaleness: caddy.Duration(2 * time.Minute),
		},
		{
			name: "full block with named port",
			input: `dynamic kubernetes my-service {
				port http
			}`,
			wantService: "my-service",
			wantPort:    "http",
		},
		{
			name:    "invalid shortcut (too many slashes)",
			input:   `dynamic kubernetes ns/svc/extra`,
			wantErr: true,
		},
		{
			name:    "invalid shortcut (too many colons)",
			input:   `dynamic kubernetes svc:80:extra`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := new(Kubernetes)
			d := caddyfile.NewTestDispenser(tt.input)
			err := k.UnmarshalCaddyfile(d)
			if (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalCaddyfile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if k.Service != tt.wantService {
				t.Errorf("Service = %v, want %v", k.Service, tt.wantService)
			}
			if k.Namespace != tt.wantNamespace {
				t.Errorf("Namespace = %v, want %v", k.Namespace, tt.wantNamespace)
			}
			if tt.wantPort != "" && k.Port != tt.wantPort {
				t.Errorf("Port = %v, want %v", k.Port, tt.wantPort)
			}
			if k.MaxStaleness != tt.wantMaxStaleness {
				t.Errorf("MaxStaleness = %v, want %v", k.MaxStaleness, tt.wantMaxStaleness)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		k       *Kubernetes
		wantErr bool
	}{
		{
			name: "valid",
			k: &Kubernetes{
				Service: "foo",
			},
			wantErr: false,
		},
		{
			name: "missing service",
			k: &Kubernetes{
				Namespace: "default",
			},
			wantErr: true,
		},
		{
			name: "invalid max_staleness (too low)",
			k: &Kubernetes{
				Service:      "foo",
				PollInterval: caddy.Duration(15 * time.Second),
				MaxStaleness: caddy.Duration(10 * time.Second),
			},
			wantErr: true,
		},
		{
			name: "valid max_staleness",
			k: &Kubernetes{
				Service:      "foo",
				PollInterval: caddy.Duration(15 * time.Second),
				MaxStaleness: caddy.Duration(30 * time.Second),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.k.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
