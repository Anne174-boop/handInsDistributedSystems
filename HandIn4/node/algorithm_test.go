package main

import "testing"

func TestHasPriority(t *testing.T) {
	tests := []struct {
		name string
		a    Request
		b    Request
		want bool
	}{
		{
			name: "laveste tidsstempel vinder",
			a:    Request{NodeID: 3, Timestamp: 4},
			b:    Request{NodeID: 1, Timestamp: 5},
			want: true,
		},
		{
			name: "hoejeste tidsstempel taber",
			a:    Request{NodeID: 1, Timestamp: 6},
			b:    Request{NodeID: 3, Timestamp: 5},
			want: false,
		},
		{
			name: "samme tidsstempel - laveste ID vinder",
			a:    Request{NodeID: 1, Timestamp: 5},
			b:    Request{NodeID: 2, Timestamp: 5},
			want: true,
		},
		{
			name: "samme tidsstempel - hoejeste ID taber",
			a:    Request{NodeID: 2, Timestamp: 5},
			b:    Request{NodeID: 1, Timestamp: 5},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasPriority(tt.a, tt.b)

			if got != tt.want {
				t.Errorf(
					"hasPriority(%+v, %+v) = %t; forventede %t",
					tt.a,
					tt.b,
					got,
					tt.want,
				)
			}
		})
	}
}
