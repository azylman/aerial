package main

import "github.com/azylman/aerial/services/prospera/internal/testutil"

var setUpTestStore = testutil.SetUpTestStore

// Retained for the chunk-08 helper purge; referenced here so the symbol
// stays live until then.
var _ = setUpTestStore
