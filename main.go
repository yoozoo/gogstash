package main

import "github.com/tsaikd/gogstash/cmd"
import "github.com/google/gops/agent"
import "log"

func main() {
	if err := agent.Listen(agent.Options{}); err != nil {
		log.Fatal(err)
	}
	cmd.Module.MustMainRun()
}
