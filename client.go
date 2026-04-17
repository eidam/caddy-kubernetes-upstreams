package kubernetes

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/caddyserver/caddy/v2"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func (k *Kubernetes) initKubernetesClient() error {
	var config *rest.Config
	var err error

	repl := caddy.NewReplacer()
	kubeconfigPath := repl.ReplaceAll(k.Kubeconfig, "")

	if kubeconfigPath != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return fmt.Errorf("failed to create kubernetes config from kubeconfig at %s: %v", kubeconfigPath, err)
		}
	} else {
		// Try in-cluster config first
		config, err = rest.InClusterConfig()
		if err != nil {
			// Fallback to standard kubeconfig location
			kubeconfig := os.Getenv("KUBECONFIG")
			if kubeconfig == "" {
				home, _ := os.UserHomeDir()
				kubeconfig = filepath.Join(home, ".kube", "config")
			}
			config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
			if err != nil {
				return fmt.Errorf("failed to create kubernetes config: %v", err)
			}
		}
	}

	k.client, err = kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %v", err)
	}

	return nil
}
