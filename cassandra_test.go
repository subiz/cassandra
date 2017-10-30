// +build ignore

package cassandra_test

import (
//	"testing"
//	. "bitbucket.org/subiz/conversation/db"
//	pb "bitbucket.org/subiz/header/conversation"
//	"github.com/thanhpk/goslice"
//	"bitbucket.org/subiz/gocommon"
//	"bitbucket.org/subiz/id"
//	commonpb "bitbucket.org/subiz/header/common"
)

var ruledb *RuleDB

func TestMain(m *testing.M) {
	seed := common.StartCassandraDev("")
	ruledb = &RuleDB{}
	ruledb.Config([]string{seed}, "casstest", 1)
	m.Run()
}

func TestRuleCrud(t *testing.T) {
	id1, id2, id3 := ID.NewRuleID(), ID.NewRuleID(), ID.NewRuleID()
	accid := ID.NewAccountID()
	st := pb.AssignStrategy_ROUNDROBINAGENTS
	ruledb.Upsert(&pb.Rule{
		Id: &id1,
		AccountId: &accid,
		PrevId: &id2,
		Strategy: &st,
		AssignTos: []string{ "agent1", "agent2", "agent3" },
		Conditions: []*pb.Condition{
			&pb.Condition{
				Key: common.AmpS("user.name"),
				Operator: common.AmpS("eq"),
				Value: common.AmpS("thanh"),
			},
		},
	})
	ruledb.Upsert(&pb.Rule{
		Ctx: &commonpb.Context{
			EventId: common.AmpS("cool"),
		},
		Id: &id2,
		AccountId: &accid,
		NextId: &id1,
		PrevId: &id3,
		Strategy: &st,
		AssignTos: []string{ "agent1", "agent2", "agent3" },
		Conditions: []*pb.Condition{
			&pb.Condition{
				Key: common.AmpS("user.email"),
				Operator: common.AmpS("superset"),
				Value: common.AmpS("[\"thanhpk@live.com\"]"),
			},
		},
	})
	ruledb.Upsert(&pb.Rule{
		NextId: &id2,
		Id: &id3,
		AccountId: &accid,
	})

	if 3 != ruledb.Count(accid) {
		t.Fatalf("should be 3")
	}

	ruledb.Delete(accid, id3)

	rules := ruledb.List(accid)
	if len(rules) != 2 {
		t.Fatalf("len should be 2, actual %d", len(rules))
	}

	rule := rules[1]
	if rule.GetId() != id2 {
		rule = rules[0]
	}
	if rule.GetId() != id2 ||
		rule.GetAccountId() != accid ||
		rule.GetStrategy() != pb.AssignStrategy_ROUNDROBINAGENTS ||
		!slice.Equal(rule.AssignTos, []string{ "agent1", "agent2", "agent3" }) ||
		rule.GetConditions()[0].GetKey() != "user.email" ||
		rule.GetCtx().GetEventId() != "cool" {
		t.Fatal("wrong storing")
	}
	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("should panic")
			}
		}()
		ruledb.Read(accid, id3)
	}()
	ruledb.Upsert(&pb.Rule{
		Id: &id2,
		AccountId: &accid,
		AssignTos: []string{ "agent1", "agent2" },
	})

	rule = ruledb.Read(accid, id2)
	if rule.GetId() != id2 ||
		rule.GetAccountId() != accid ||
		rule.GetStrategy() != pb.AssignStrategy_ROUNDROBINAGENTS ||
		!slice.Equal(rule.AssignTos, []string{ "agent1", "agent2" }) ||
		rule.GetConditions()[0].GetKey() != "user.email" ||
		rule.GetCtx().GetEventId() != "cool" {
		t.Fatal("wrong storing")
	}
}
