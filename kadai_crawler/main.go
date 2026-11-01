package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type SafeUrlMap struct {
	visited map[string]bool
	mu      sync.Mutex
}

// Visitは未訪問のURLを記録し、今回初めて訪問する場合にtrueを返す。
func (m *SafeUrlMap) Visit(url string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.visited[url] {
		return false
	}
	if m.visited == nil {
		m.visited = make(map[string]bool)
	}
	m.visited[url] = true
	return true
}

// URLを受け取り、そのページに含まれるリンク（文字列のスライス）を返します
func fetch(url string) []string {
	// 処理時間をランダムにシミュレーション（0〜1.3秒）
	// ※タイムアウト(1秒)の挙動を確認するため
	time.Sleep(time.Duration(rand.Intn(1300)) * time.Millisecond)

	// 擬似的なリンクデータ
	links := map[string][]string{
		"https://example.com":   {"https://example.com/a", "https://example.com/b"},
		"https://example.com/a": {"https://example.com/c"},
		"https://example.com/b": {"https://example.com/a"}, // 重複リンク
		"https://example.com/c": {},
	}

	return links[url]
}

func Crawl(url string, visited *SafeUrlMap, wg *sync.WaitGroup) {
	defer wg.Done()
	if !visited.Visit(url) {
		fmt.Println("重複スキップ:", url)
		return
	}

	fmt.Println("取得開始:", url)
	result := make(chan []string, 1)
	timeout := time.After(1 * time.Second)

	// fetchはキャンセルできないため、その終了もWaitGroupで待機する。
	// バッファにより、タイムアウト後も結果の送信でブロックしない。
	wg.Add(1)
	go func() {
		defer wg.Done()
		result <- fetch(url)
	}()

	select {
	case links := <-result:
		fmt.Printf("取得成功: %s -> リンク数: %d\n", url, len(links))
		for _, link := range links {
			wg.Add(1)
			go Crawl(link, visited, wg)
		}
	case <-timeout:
		fmt.Println("タイムアウト:", url)
	}
}

func main() {
	visited := &SafeUrlMap{visited: make(map[string]bool)}
	var wg sync.WaitGroup
	wg.Add(1)
	go Crawl("https://example.com", visited, &wg)
	wg.Wait()
	fmt.Println("すべてのクロールが完了しました")
}
