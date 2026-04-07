package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	// Конфигурация из ENV или дефолты для теста
	clusterNodes := getEnv("REDIS_CLUSTER_NODES", "localhost:6381,localhost:6382,localhost:6383")
	username := getEnv("REDIS_USERNAME", "")       // пустой для Redis без ACL
	password := getEnv("REDIS_PASSWORD", "testpass123")

	nodes := strings.Split(clusterNodes, ",")

	fmt.Println("=== Redis Cluster Connection Test ===")
	fmt.Printf("Nodes: %v\n", nodes)
	fmt.Printf("Username: '%s'\n", username)
	fmt.Printf("Password: '%s'\n", maskPassword(password))

	// Создаём клиент
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    nodes,
		Username: username,
		Password: password,
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Тест 1: Ping
	fmt.Println("\n[Test 1] Ping...")
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("PING failed: %v", err)
	}
	fmt.Println("✓ PING successful")

	// Тест 2: Cluster Info
	fmt.Println("\n[Test 2] Cluster Info...")
	info, err := client.ClusterInfo(ctx).Result()
	if err != nil {
		log.Fatalf("CLUSTER INFO failed: %v", err)
	}
	// Показываем первые несколько строк
	lines := strings.Split(info, "\n")
	for i, line := range lines {
		if i >= 5 {
			break
		}
		fmt.Printf("  %s\n", line)
	}
	fmt.Println("✓ CLUSTER INFO successful")

	// Тест 3: Set/Get
	fmt.Println("\n[Test 3] SET/GET...")
	testKey := "test:cluster:key"
	testValue := "hello-from-go-client"

	if err := client.Set(ctx, testKey, testValue, time.Minute).Err(); err != nil {
		log.Fatalf("SET failed: %v", err)
	}
	fmt.Printf("✓ SET %s = %s\n", testKey, testValue)

	result, err := client.Get(ctx, testKey).Result()
	if err != nil {
		log.Fatalf("GET failed: %v", err)
	}
	fmt.Printf("✓ GET %s = %s\n", testKey, result)

	if result != testValue {
		log.Fatalf("Value mismatch: expected %s, got %s", testValue, result)
	}

	// Тест 4: Delete
	fmt.Println("\n[Test 4] DEL...")
	if err := client.Del(ctx, testKey).Err(); err != nil {
		log.Fatalf("DEL failed: %v", err)
	}
	fmt.Printf("✓ DEL %s\n", testKey)

	fmt.Println("\n=== All tests passed! ===")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func maskPassword(pass string) string {
	if len(pass) <= 4 {
		return "****"
	}
	return pass[:2] + "****" + pass[len(pass)-2:]
}
