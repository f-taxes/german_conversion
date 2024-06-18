package grpc_client

import (
	"context"
	"time"

	"github.com/f-taxes/german_conversion/global"
	"github.com/f-taxes/german_conversion/proto"
	"github.com/kataras/golog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

var GrpcClient *FTaxesClient

type FTaxesClient struct {
	conStr     string
	Connection *grpc.ClientConn
	GrpcClient proto.FTaxesClient
}

func NewFTaxesClient(conStr string) *FTaxesClient {
	return &FTaxesClient{
		conStr: conStr,
	}
}

func (c *FTaxesClient) Connect(ctx context.Context) error {
	con, err := grpc.DialContext(ctx, c.conStr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithConnectParams(grpc.ConnectParams{
		MinConnectTimeout: time.Second * 30,
		Backoff:           backoff.Config{MaxDelay: time.Second},
	}))

	if err != nil {
		golog.Errorf("Failed to establish grpc connections: %v", err)
		return err
	}

	go func() {
		state := con.GetState()
		for {
			golog.Infof("Connection state: %s", state.String())
			con.WaitForStateChange(context.Background(), state)
			state = con.GetState()
		}
	}()

	c.Connection = con
	c.GrpcClient = proto.NewFTaxesClient(con)

	return nil
}

func (c *FTaxesClient) ShowJobProgress(ctx context.Context, job *proto.JobProgress) error {
	job.Plugin = global.Plugin.Label
	_, err := c.GrpcClient.ShowJobProgress(ctx, job)
	return err
}

func (c *FTaxesClient) PluginHeartbeat(ctx context.Context) error {
	_, err := c.GrpcClient.PluginHeartbeat(ctx, &proto.PluginInfo{ID: global.Plugin.ID, Version: global.Plugin.Version, HasCtlServer: global.Plugin.Ctl.Address != ""})
	return err
}
