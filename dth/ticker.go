package dth

import (
	"log"
	"time"
)

const INTERVAL_PERIOD time.Duration = 24 * time.Hour

const HOUR_TO_TICK int = 02
const MINUTE_TO_TICK int = 00
const SECOND_TO_TICK int = 00

type JobTicker struct {
	timer *time.Timer
}

func (t *JobTicker) updateTimer() {
	nextTick := time.Date(time.Now().Year(), time.Now().Month(),
		time.Now().Day(), HOUR_TO_TICK, MINUTE_TO_TICK, SECOND_TO_TICK, 0, time.Local)
	if !nextTick.After(time.Now()) {
		nextTick = nextTick.Add(INTERVAL_PERIOD)
	}
	log.Printf("next tick is:%v", nextTick)
	diff := nextTick.Sub(time.Now())
	if diff < 0 {
		diff = INTERVAL_PERIOD
	}
	if t.timer == nil {
		t.timer = time.NewTimer(diff)
	} else {
		t.timer.Reset(diff)
	}
}
