package collector

import (
	"database/sql"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/smartystreets/goconvey/convey"
)

const dsn = "root@/mysql"

func TestExporter(t *testing.T) {
	if testing.Short() {
		t.Skip("-short is passed, skipping test")
	}

	exporter := New(
		dsn,
		NewMetrics(),
		[]Scraper{
			ScrapeGlobalStatus{},
		})

	convey.Convey("Metrics describing", t, func() {
		ch := make(chan *prometheus.Desc)
		go func() {
			exporter.Describe(ch)
			close(ch)
		}()

		for range ch {
		}
	})

	convey.Convey("Metrics collection", t, func() {
		ch := make(chan prometheus.Metric)
		go func() {
			exporter.Collect(ch)
			close(ch)
		}()

		for m := range ch {
			got := readMetric(m)
			if got.labels[model.MetricNameLabel] == "mysql_up" {
				convey.So(got.value, convey.ShouldEqual, 1)
			}
		}
	})
}

func TestGetMySQLVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("-short is passed, skipping test")
	}

	convey.Convey("Version parsing", t, func() {
		db, err := sql.Open("mysql", dsn)
		convey.So(err, convey.ShouldBeNil)
		defer db.Close()

		convey.So(getMySQLVersion(db), convey.ShouldBeBetweenOrEqual, 5.5, 10.3)
	})
}
