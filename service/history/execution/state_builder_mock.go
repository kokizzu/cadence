// The MIT License (MIT)

// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Code generated by MockGen. DO NOT EDIT.
// Source: state_builder.go
//
// Generated by this command:
//
//	mockgen -package execution -source state_builder.go -destination state_builder_mock.go -self_package github.com/uber/cadence/service/history/execution
//

// Package execution is a generated GoMock package.
package execution

import (
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"

	types "github.com/uber/cadence/common/types"
)

// MockStateBuilder is a mock of StateBuilder interface.
type MockStateBuilder struct {
	ctrl     *gomock.Controller
	recorder *MockStateBuilderMockRecorder
	isgomock struct{}
}

// MockStateBuilderMockRecorder is the mock recorder for MockStateBuilder.
type MockStateBuilderMockRecorder struct {
	mock *MockStateBuilder
}

// NewMockStateBuilder creates a new mock instance.
func NewMockStateBuilder(ctrl *gomock.Controller) *MockStateBuilder {
	mock := &MockStateBuilder{ctrl: ctrl}
	mock.recorder = &MockStateBuilderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockStateBuilder) EXPECT() *MockStateBuilderMockRecorder {
	return m.recorder
}

// ApplyEvents mocks base method.
func (m *MockStateBuilder) ApplyEvents(domainID, requestID string, workflowExecution types.WorkflowExecution, history, newRunHistory []*types.HistoryEvent) (MutableState, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ApplyEvents", domainID, requestID, workflowExecution, history, newRunHistory)
	ret0, _ := ret[0].(MutableState)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ApplyEvents indicates an expected call of ApplyEvents.
func (mr *MockStateBuilderMockRecorder) ApplyEvents(domainID, requestID, workflowExecution, history, newRunHistory any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ApplyEvents", reflect.TypeOf((*MockStateBuilder)(nil).ApplyEvents), domainID, requestID, workflowExecution, history, newRunHistory)
}

// GetMutableState mocks base method.
func (m *MockStateBuilder) GetMutableState() MutableState {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMutableState")
	ret0, _ := ret[0].(MutableState)
	return ret0
}

// GetMutableState indicates an expected call of GetMutableState.
func (mr *MockStateBuilderMockRecorder) GetMutableState() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMutableState", reflect.TypeOf((*MockStateBuilder)(nil).GetMutableState))
}
