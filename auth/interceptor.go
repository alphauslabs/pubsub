package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/golang/glog"
	"google.golang.org/api/idtoken"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var unauthorizedCallerErr = status.Errorf(codes.Unauthenticated, "Unauthorized caller.")

var allowed = []string{
	"@labs-169405.iam.gserviceaccount.com",
	"@mobingi-main.iam.gserviceaccount.com",
	"compute@developer.gserviceaccount.com",
}

type userInfo struct {
	Email string
}

func verifyCaller(ctx context.Context, md metadata.MD) (userInfo, error) {
	glog.Infof("metadata: %v", md)

	var token string
	v := md.Get("authorization")
	if len(v) > 0 {
		tt := strings.Split(v[0], " ")
		if strings.ToLower(tt[0]) == "bearer" {
			token = tt[1]
		}
	}

	if token == "" {
		glog.Errorf("failed: unauthorized call")
		return userInfo{}, unauthorizedCallerErr
	}

	payload, err := idtoken.Validate(ctx, token, "35.213.124.15:50051")
	if err != nil {
		glog.Errorf("Validate failed: %v", err)
		return userInfo{}, err
	}

	b, _ := json.Marshal(payload)
	glog.Infof("payload=%v", string(b))
	glog.Infof("claims=%v", payload.Claims)

	var emailVerified bool
	if v, ok := payload.Claims["email_verified"]; ok {
		emailVerified = v.(bool)
	}

	if !emailVerified {
		glog.Errorf("failed: email not verified")
		return userInfo{}, unauthorizedCallerErr
	}

	var email string
	if v, ok := payload.Claims["email"]; ok {
		email = fmt.Sprintf("%v", v)
	}

	var validEmail bool
	for _, allow := range allowed {
		if strings.HasSuffix(email, allow) {
			validEmail = validEmail || true
		}
	}

	if !validEmail {
		glog.Errorf("failed: invalid email")
		return userInfo{}, unauthorizedCallerErr
	}

	return userInfo{Email: email}, nil
}

func UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (interface{}, error) {
	defer func(begin time.Time) {
		glog.Infof("[unary] << %v duration: %v", info.FullMethod, time.Since(begin))
	}(time.Now())

	glog.Infof("[unary] >> %v", info.FullMethod)
	md, _ := metadata.FromIncomingContext(ctx)
	u, err := verifyCaller(ctx, md)
	if err != nil {
		return nil, unauthorizedCallerErr
	}

	nctx := metadata.NewIncomingContext(ctx, md)
	nctx = context.WithValue(nctx, "email", u.Email)
	nctx = context.WithValue(nctx, "fullMethod", info.FullMethod)
	return h(nctx, req)
}

type StreamContextWrapper interface {
	grpc.ServerStream
	SetContext(context.Context)
}

type wrapper struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrapper) Context() context.Context {
	return w.ctx
}

func (w *wrapper) SetContext(ctx context.Context) {
	w.ctx = ctx
}

func newStreamContextWrapper(inner grpc.ServerStream) StreamContextWrapper {
	ctx := inner.Context()
	return &wrapper{inner, ctx}
}

func StreamInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	defer func(begin time.Time) {
		glog.Infof("[stream] << %v duration: %v", info.FullMethod, time.Since(begin))
	}(time.Now())

	glog.Infof("[stream] >> %v", info.FullMethod)
	md, _ := metadata.FromIncomingContext(stream.Context())
	u, err := verifyCaller(stream.Context(), md)
	if err != nil {
		return unauthorizedCallerErr
	}

	wrap := newStreamContextWrapper(stream)
	nctx := context.WithValue(wrap.Context(), "email", u.Email)
	nctx = context.WithValue(nctx, "fullMethod", info.FullMethod)
	wrap.SetContext(nctx)
	return h(srv, wrap)
}
