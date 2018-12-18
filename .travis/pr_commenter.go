package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	// Find all env variables
	token := os.Getenv("GITHUB_COMMENT_TOKEN")
	if token == "" {
		fmt.Fprintf(os.Stderr, "GITHUB_COMMENT_TOKEN was not provided")
		os.Exit(0)
	}
	repo := os.Getenv("TRAVIS_REPO_SLUG")
	if repo == "" {
		fmt.Fprintf(os.Stderr, "TRAVIS_REPO_SLUG was not provided")
		os.Exit(0)
	}
	pr := os.Getenv("TRAVIS_PULL_REQUEST")
	if pr == "" {
		fmt.Fprintf(os.Stderr, "TRAVIS_PULL_REQUEST was not provided")
		os.Exit(0)
	}
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "CLI args were not provided")
		os.Exit(0)
	}
	name := os.Args[1]
	filePath := os.Args[2]
	content, err := ioutil.ReadFile(filePath)
	panicOnErr(err)
	if content[len(content)-1] == byte('\n') {
		content = content[:len(content)-1]
	}

	// Build json
	b := new(strings.Builder)
	b.WriteString("<details>\n")
	b.WriteString(" <summary>" + name + " results </summary>\n\n")
	b.WriteString("```gf\n")
	b.WriteString(string(content))
	b.WriteString("\n```")
	type jsonStruct struct {
		Body string `json:"body"`
	}
	j := jsonStruct{Body: b.String()}
	encoded, err := json.Marshal(&j)
	panicOnErr(err)
	reader := bytes.NewReader(encoded)

	// Do request
	url := fmt.Sprintf("https://api.github.com/repos/%s/issues/%v/comments", repo, pr)
	client := new(http.Client)
	req, err := http.NewRequest("POST", url, reader)
	panicOnErr(err)
	req.Header.Set("Content-Type", "encoding/json")
	req.Header.Set("Authorization", "token "+token)
	resp, err := client.Do(req)
	panicOnErr(err)
	resp.Body.Close()
	fmt.Printf("Comment for %s submitted!\n", name)
}
