package api

import (
	"context"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	ch "analytics/internal/clickhouse"
	"analytics/internal/metrics"
)

type Server struct {
	echo        *echo.Echo
	ch          *ch.Client
	logger      *logrus.Logger
	promHandler http.Handler
}

func New(chClient *ch.Client, logger *logrus.Logger, reg prometheus.Gatherer) *Server {
	e := echo.New()
	s := &Server{
		echo:        e,
		ch:          chClient,
		logger:      logger,
		promHandler: metrics.Handler(reg),
	}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.echo.GET("/healthz", s.healthz)
	s.echo.GET("/aggregates", s.aggregates)
	s.echo.GET("/metrics", s.metrics)
}

func (s *Server) healthz(c echo.Context) error {
	ctx := c.Request().Context()
	if err := s.ch.Ping(ctx); err != nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]string{"status": "error", "error": err.Error()})
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) aggregates(c echo.Context) error {
	chatID := c.QueryParam("chat_id")
	since, err := parseDate(c.QueryParam("since"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid since date"})
	}
	until, err := parseDate(c.QueryParam("until"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid until date"})
	}
	if chatID == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "chat_id, since, and until are required"})
	}

	items, err := s.ch.DailyCounts(c.Request().Context(), chatID, since, until)
	if err != nil {
		s.logger.WithError(err).Error("daily counts query failed")
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to query aggregates"})
	}

	return c.JSON(http.StatusOK, items)
}

func (s *Server) metrics(c echo.Context) error {
	s.promHandler.ServeHTTP(c.Response().Writer, c.Request())
	return nil
}

func parseDate(v string) (time.Time, error) {
	return time.ParseInLocation("2006-01-02", v, time.UTC)
}

func (s *Server) Start(address string) error {
	return s.echo.Start(address)
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.echo.Shutdown(ctx)
}
