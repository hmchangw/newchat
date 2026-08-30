//go:build integration

package testutil

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"testing"

	vaultapi "github.com/hashicorp/vault/api"
	tcvault "github.com/testcontainers/testcontainers-go/modules/vault"

	"github.com/hmchangw/chat/pkg/testutil/testimages"
)

// VaultHandle is the per-test view of the shared Vault container. The
// transit key it points at is provisioned freshly for each test, so
// rotation and other in-place mutations don't bleed between tests.
type VaultHandle struct {
	Address      string
	Token        string
	TransitMount string
	TransitKey   string

	client *vaultapi.Client
}

const (
	// #nosec G101 -- root token for an ephemeral test container, not a production credential
	// nosemgrep: gosec.G101-1
	vaultRootToken    = "test-root-token"
	vaultTransitMount = "transit"
)

// vaultBase is the shared, process-wide Vault handle: client, address,
// root token, and the (single) transit mount. Per-test keys are created
// on top of this. The backing Vault instance is either a `vault server
// -dev` subprocess (when the binary is on PATH) or a testcontainers
// Vault container.
type vaultBase struct {
	client  *vaultapi.Client
	address string
}

var (
	vaultOnce    sync.Once
	vaultBaseRef *vaultBase
	vaultInitErr error
	vaultStop    func()
)

func ensureVaultBase(ctx context.Context) (*vaultBase, error) {
	vaultOnce.Do(func() {
		var addr string
		if a, stop, err := startVaultDevBinary(); err == nil {
			addr = a
			vaultStop = stop
		} else {
			container, cerr := tcvault.Run(
				ctx,
				testimages.Vault,
				tcvault.WithToken(vaultRootToken),
			)
			if cerr != nil {
				vaultInitErr = fmt.Errorf("start vault: %w", cerr)
				return
			}
			caddr, cerr := container.HttpHostAddress(ctx)
			if cerr != nil {
				_ = container.Terminate(context.Background())
				vaultInitErr = fmt.Errorf("get vault address: %w", cerr)
				return
			}
			addr = caddr
			vaultStop = func() { _ = container.Terminate(context.Background()) }
		}

		cfg := vaultapi.DefaultConfig()
		cfg.Address = addr
		client, err := vaultapi.NewClient(cfg)
		if err != nil {
			vaultStop()
			vaultStop = nil
			vaultInitErr = fmt.Errorf("vault client: %w", err)
			return
		}
		client.SetToken(vaultRootToken)

		// Enable the transit engine once for the process. The transit
		// engine is a mount; the keys under it are namespaced per test.
		if err := client.Sys().MountWithContext(ctx, vaultTransitMount, &vaultapi.MountInput{
			Type: "transit",
		}); err != nil {
			vaultStop()
			vaultStop = nil
			vaultInitErr = fmt.Errorf("mount transit: %w", err)
			return
		}

		vaultBaseRef = &vaultBase{client: client, address: addr}
	})
	return vaultBaseRef, vaultInitErr
}

// TerminateVault stops the shared Vault instance (subprocess or
// container). Best-effort, idempotent. Wired into TerminateAll.
func TerminateVault() {
	if vaultStop == nil {
		return
	}
	vaultStop()
	vaultStop = nil
}

// Vault returns a Vault handle whose transit key is provisioned freshly
// for this test (named from a hash of t.Name() so tests stay isolated
// even under -parallel). The container is process-shared via sync.Once
// for speed; only the transit key itself is per-test.
//
// The key is deleted on t.Cleanup so the dev Vault doesn't accumulate
// keys across a large test suite. Deletion requires the key to be
// configured with deletion_allowed=true, which we set at creation time.
// testNameHash returns a short, filesystem- and Vault-safe hex digest of
// t.Name(). FNV-64a keeps the result well under Vault's 255-char limit and
// turns any "/", " ", etc. in the test name into hex digits, giving each
// test an isolated, deterministic suffix for its keys/policies/roles.
func testNameHash(t *testing.T) string {
	t.Helper()
	h := fnv.New64a()
	_, _ = h.Write([]byte(t.Name())) // hash.Hash.Write never errors
	return fmt.Sprintf("%x", h.Sum64())
}

func Vault(t *testing.T, ctx context.Context) *VaultHandle {
	t.Helper()
	base, err := ensureVaultBase(ctx)
	if err != nil {
		t.Fatalf("testutil.Vault: %v", err)
	}

	// Per-test transit key name derived from t.Name() so tests stay isolated.
	keyName := "test-key-" + testNameHash(t)

	if _, err := base.client.Logical().WriteWithContext(ctx,
		fmt.Sprintf("%s/keys/%s", vaultTransitMount, keyName),
		map[string]any{"type": "aes256-gcm96"},
	); err != nil {
		t.Fatalf("testutil.Vault: create transit key %s: %v", keyName, err)
	}

	// Allow deletion so cleanup can drop the key. Without this Vault
	// refuses /sys/keys/<name> DELETE with "deletion is not allowed".
	if _, err := base.client.Logical().WriteWithContext(ctx,
		fmt.Sprintf("%s/keys/%s/config", vaultTransitMount, keyName),
		map[string]any{"deletion_allowed": true},
	); err != nil {
		t.Fatalf("testutil.Vault: allow deletion on %s: %v", keyName, err)
	}

	t.Cleanup(func() {
		if _, err := base.client.Logical().DeleteWithContext(ctx,
			fmt.Sprintf("%s/keys/%s", vaultTransitMount, keyName),
		); err != nil {
			t.Logf("testutil.Vault cleanup: delete %s: %v", keyName, err)
		}
	})

	return &VaultHandle{
		Address:      base.address,
		Token:        vaultRootToken,
		TransitMount: vaultTransitMount,
		TransitKey:   keyName,
		client:       base.client,
	}
}

// EnableAppRole provisions an AppRole on the shared Vault scoped to this
// handle's per-test transit key and returns a freshly-minted
// (roleID, secretID) pair for it. The approle auth method is mounted once
// per process (tolerating "path is already in use" from sibling tests);
// the policy and role are named from a hash of t.Name() so concurrent
// tests stay isolated. The role is granted exactly the transit
// capabilities the atrest KeyWrapper needs — datakey, encrypt, decrypt —
// against this handle's key and nothing else. Policy and role are dropped
// on t.Cleanup.
func (v *VaultHandle) EnableAppRole(t *testing.T, ctx context.Context) (roleID, secretID string) {
	t.Helper()

	if err := v.client.Sys().EnableAuthWithOptionsWithContext(ctx, "approle", &vaultapi.EnableAuthOptions{
		Type: "approle",
	}); err != nil && !strings.Contains(err.Error(), "path is already in use") {
		t.Fatalf("testutil.EnableAppRole: enable approle: %v", err)
	}

	suffix := testNameHash(t)
	policyName := "test-policy-" + suffix
	roleName := "test-role-" + suffix

	policy := fmt.Sprintf(`
path "%[1]s/datakey/wrapped/%[2]s" { capabilities = ["update"] }
path "%[1]s/encrypt/%[2]s"         { capabilities = ["update"] }
path "%[1]s/decrypt/%[2]s"         { capabilities = ["update"] }
`, v.TransitMount, v.TransitKey)
	if err := v.client.Sys().PutPolicyWithContext(ctx, policyName, policy); err != nil {
		t.Fatalf("testutil.EnableAppRole: put policy %s: %v", policyName, err)
	}

	if _, err := v.client.Logical().WriteWithContext(ctx, "auth/approle/role/"+roleName, map[string]any{
		"token_policies": []string{policyName},
		"token_ttl":      "10m",
		"token_max_ttl":  "30m",
		"secret_id_ttl":  "20m",
	}); err != nil {
		t.Fatalf("testutil.EnableAppRole: create role %s: %v", roleName, err)
	}

	t.Cleanup(func() {
		_, _ = v.client.Logical().DeleteWithContext(ctx, "auth/approle/role/"+roleName)
		_ = v.client.Sys().DeletePolicyWithContext(ctx, policyName)
	})

	ridResp, err := v.client.Logical().ReadWithContext(ctx, "auth/approle/role/"+roleName+"/role-id")
	if err != nil || ridResp == nil {
		t.Fatalf("testutil.EnableAppRole: read role-id: %v", err)
	}
	roleID, _ = ridResp.Data["role_id"].(string)

	sidResp, err := v.client.Logical().WriteWithContext(ctx, "auth/approle/role/"+roleName+"/secret-id", map[string]any{})
	if err != nil || sidResp == nil {
		t.Fatalf("testutil.EnableAppRole: generate secret-id: %v", err)
	}
	secretID, _ = sidResp.Data["secret_id"].(string)

	if roleID == "" || secretID == "" {
		t.Fatalf("testutil.EnableAppRole: empty role-id or secret-id")
	}
	return roleID, secretID
}

// Rotate triggers a transit-key rotation on this handle's per-test key,
// producing a new key version. Existing ciphertext continues to decrypt;
// new encryptions use the latest version.
func (v *VaultHandle) Rotate(ctx context.Context) error {
	_, err := v.client.Logical().WriteWithContext(ctx,
		fmt.Sprintf("%s/keys/%s/rotate", v.TransitMount, v.TransitKey),
		map[string]any{},
	)
	if err != nil {
		return fmt.Errorf("rotate transit key %s: %w", v.TransitKey, err)
	}
	return nil
}
