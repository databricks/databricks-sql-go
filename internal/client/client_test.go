package client

import "testing"

func TestSprintByteId(t *testing.T) {
	type args struct {
		bts []byte
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "valid case",
			args: args{[]byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 3, 4, 4, 223, 34, 54}},
			want: "01020304-0217-0402-0301-02030404df22",
		},
		{
			name: "invalid case",
			args: args{[]byte{23}},
			want: "17",
		},
		{
			name: "invalid case",
			args: args{[]byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 3, 4, 4, 223, 34, 54, 78}},
			want: "0102030402170402030102030404df22364e",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SprintByteId(tt.args.bts); got != tt.want {
				t.Errorf("SprintByteId() = %v, want %v", got, tt.want)
			}
		})
	}
}
