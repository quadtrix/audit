// package audit implements auditing
package audit

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/quadtrix/basicqueue"
)

const (
	AU_SYSTEM int = 1
)

type AuditEntry struct {
	timestamp string
	source    string
	action    string
	change    string
	user      string
}
type Audit struct {
	asyncQueryQueue  *basicqueue.BasicQueue
	stopNotifier     *basicqueue.BasicQueue
	eventQueue       *basicqueue.BasicQueue
	queue_identifier string
}

func New(stopNotifier *basicqueue.BasicQueue, asyncQueryQueue *basicqueue.BasicQueue, eventQueue *basicqueue.BasicQueue) (aul *Audit, err error) {
	au := Audit{}
	au.queue_identifier = fmt.Sprintf("audit_%s", uuid.New().String())
	au.stopNotifier = stopNotifier
	err = au.stopNotifier.RegisterConsumer(au.queue_identifier)
	if err != nil {
		return &au, err
	}
	au.asyncQueryQueue = asyncQueryQueue
	err = au.asyncQueryQueue.RegisterProducer(au.queue_identifier)
	if err != nil {
		return &au, err
	}
	au.eventQueue = eventQueue
	err = au.eventQueue.RegisterProducer(au.queue_identifier)
	if err != nil {
		return &au, err
	}
	go au.queuePolling()
	return &au, nil
}

func (au *Audit) AuditLog(timestamp string, source string, action string, change string, user int) {
	au.asyncQueryQueue.AddJsonMessage(au.queue_identifier, "audit", "mysqldb", "QUERY", fmt.Sprintf("INSERT INTO auditing (tstamp, source, paction, pchange, user) VALUES(\"%s\", \"%s\", \"%s\", \"%s\", %d)", timestamp, source, action, change, user))
}

func (au Audit) queuePolling() {
	var stopHistory []string
	for {
		if au.stopNotifier.PollWithHistory(au.queue_identifier, stopHistory) {
			message, err := au.stopNotifier.ReadJsonWithHistory(au.queue_identifier, stopHistory)
			if err != nil {
				continue // ignore this bad read (we will be killed anyway)
			}
			stopHistory = append(stopHistory, message.MessageID)
			if message.Destination == "audit" || message.Destination == "all" {
				switch message.MessageType {
				case "STOP":
					auditDone := false
					for !auditDone {
						msgcount, _, _, _, _ := au.asyncQueryQueue.QStats(au.queue_identifier)
						if msgcount == 0 {
							auditDone = true
						}
						time.Sleep(10 * time.Millisecond)
					}
					au.eventQueue.AddJsonMessage(au.queue_identifier, "audit", "main", "AUDITSTOPPED", "")
					break
				}
			}
		}
		time.Sleep(time.Second)
	}
}
