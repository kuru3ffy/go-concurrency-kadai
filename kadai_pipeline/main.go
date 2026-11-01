package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Result struct {
	Path  string
	Words int
}

// テスト用のダミーファイルを作成する関数
func generateDummyFiles(dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	// 5つのファイルを作成
	for i := 1; i <= 5; i++ {
		filename := filepath.Join(dir, fmt.Sprintf("file%d.txt", i))
		// ダミーの単語データ（"word" という単語を i*10 個書き込む）
		content := strings.Repeat("word ", i*10)
		if err := os.WriteFile(filename, []byte(content), 0644); err != nil {
			return err
		}
	}
	return nil
}

func findFiles(ctx context.Context, root string, paths chan<- string) {
	defer close(paths)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if d.Type().IsRegular() && filepath.Ext(path) == ".txt" {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case paths <- path:
			}
		}
		return nil
	})
	if err != nil {
		fmt.Printf("探索エラー: %v\n", err)
	}
}

func countFile(ctx context.Context, path string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)
	count := 0
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
		count++
	}
	return count, scanner.Err()
}

func countWords(ctx context.Context, paths <-chan string, results chan<- Result) {
	for {
		select {
		case <-ctx.Done():
			return
		case path, ok := <-paths:
			if !ok {
				return
			}
			words, err := countFile(ctx, path)
			if err != nil {
				fmt.Printf("集計エラー: %s: %v\n", path, err)
				continue
			}
			select {
			case <-ctx.Done():
				return
			case results <- Result{Path: path, Words: words}:
			}
		}
	}
}

func run() error {
	timeout := flag.Duration("timeout", 30*time.Second, "処理の制限時間（例：30s、1us）")
	dir := flag.String("dir", "", "探索するディレクトリ（省略時はダミーファイルを生成）")
	flag.Parse()
	root := *dir
	if root == "" {
		var err error
		root, err = os.MkdirTemp("", "word-pipeline-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(root)
		if err := generateDummyFiles(root); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	paths := make(chan string)
	results := make(chan Result)
	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		defer wg.Done()
		findFiles(ctx, root, paths)
	}()
	for i := 0; i < 4; i++ {
		go func() {
			defer wg.Done()
			countWords(ctx, paths, results)
		}()
	}

	// 終了監視もContextを受け取り、キャンセル時も全ステージの後片付けを待つ。
	done := make(chan struct{})
	go func(ctx context.Context) {
		defer close(done)
		select {
		case <-ctx.Done():
			// キャンセル済みでも、ワーカーが終了するまで待ってから閉じる。
		default:
		}
		wg.Wait()
		close(results)
	}(ctx)

	files, total := 0, 0
	for result := range results {
		fmt.Printf("%s: %d単語\n", result.Path, result.Words)
		files++
		total += result.Words
	}
	<-done
	if ctx.Err() != nil {
		fmt.Printf("キャンセル: %v\n", ctx.Err())
	}
	fmt.Printf("処理完了\n処理ファイル数: %d\n総単語数: %d\n", files, total)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
