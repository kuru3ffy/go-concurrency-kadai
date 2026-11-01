package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCountFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "words.txt")
	// 長い単語と日本語の空白も含め、Scannerの既定の上限を超える。
	content := strings.Repeat("a", 70000) + " end\nこんにちは\u3000世界"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := countFile(context.Background(), path)
	if err != nil || got != 4 {
		t.Fatalf("countFile() = %d, %v; want 4, nil", got, err)
	}
}

func TestCountWordsReportsReadError(t *testing.T) {
	paths := make(chan string, 1)
	results := make(chan Result, 1)
	missing := filepath.Join(t.TempDir(), "missing.txt")
	paths <- missing
	close(paths)
	countWords(context.Background(), paths, results)
	result := <-results
	if result.Path != missing || !errors.Is(result.Err, os.ErrNotExist) {
		t.Fatalf("countWords() = %+v; want missing-file error", result)
	}
}

func TestFindFilesCancellationWhileSending(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	paths := make(chan string)
	done := make(chan error, 1)
	go func() { done <- findFiles(ctx, root, paths) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("findFiles() error = %v; want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("findFiles did not exit after cancellation")
	}
}

func TestCountFileCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := countFile(ctx, "unused.txt"); err != context.Canceled {
		t.Fatalf("countFile() error = %v; want context.Canceled", err)
	}
}

func TestFindFilesNested(t *testing.T) {
	root := t.TempDir()
	child := filepath.Join(root, "nested")
	if err := os.Mkdir(child, 0700); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(child, "sample.txt")
	for _, path := range []string{want, filepath.Join(root, "skip.csv")} {
		if err := os.WriteFile(path, []byte("sample"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	paths := make(chan string, 2)
	if err := findFiles(context.Background(), root, paths); err != nil {
		t.Fatal(err)
	}
	var got []string
	for path := range paths {
		got = append(got, path)
	}
	if len(got) != 1 || got[0] != want {
		t.Fatalf("findFiles() = %v; want [%s]", got, want)
	}
}
