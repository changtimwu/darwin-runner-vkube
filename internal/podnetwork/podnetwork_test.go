package podnetwork

import (
	"context"
	"net/netip"
	"testing"
	"time"
)

func testNetwork() *Network {
	n := New(Config{
		NodeIP:      netip.MustParseAddr("192.168.1.101"),
		ClusterCIDR: netip.MustParsePrefix("10.42.0.0/16"),
		ServiceCIDR: netip.MustParsePrefix("10.43.0.0/16"),
	})
	n.dnsVIP = "10.43.0.10"
	return n
}

func TestEnv(t *testing.T) {
	env := testNetwork().Env(netip.MustParseAddr("10.42.1.4"), "team")
	for k, want := range map[string]string{
		"K3SM_POD_IP":        "10.42.1.4",
		"K3SM_CLUSTER_CIDRS": "10.42.0.0/16,10.43.0.0/16",
		"K3SM_DNS_SERVER":    "10.43.0.10",
		"K3SM_DNS_DOMAIN":    "cluster.local",
	} {
		if env[k] != want {
			t.Errorf("%s = %q, want %q", k, env[k], want)
		}
	}
	if s := env["K3SM_DNS_SEARCH"]; s == "" || s[:len("team.svc.cluster.local")] != "team.svc.cluster.local" {
		t.Errorf("search list %q does not start with the pod's namespace", s)
	}
}

func TestHostNetworkPodsKeepTheNodeIP(t *testing.T) {
	n := testNetwork()
	n.SetHostNetwork("p1")
	// Not ready: a hostNetwork pod must not wait for the pod network.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if ip, err := n.Setup(ctx, "p1"); err != nil || ip != "192.168.1.101" {
		t.Errorf("Setup = %q, %v; want the node IP", ip, err)
	}
	if ok, err := n.Reattach(ctx, "p1", netip.MustParseAddr("192.168.1.101"), false, "default"); ok || err != nil {
		t.Errorf("Reattach = %v, %v; want nothing to re-claim", ok, err)
	}
	if err := n.Teardown("p1"); err != nil {
		t.Error(err)
	}
	if _, err := n.Setup(ctx, "p2"); err == nil {
		t.Error("a pod got an IP before the network was up")
	}
}
