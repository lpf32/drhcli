package drh

import (
	"reflect"
	"testing"
)

func TestNewObject(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name  string
		args  args
		wantO *Object
	}{
		{
			name: "Test1",
			args: args{
				str: `{"Key":"ABC","Size":100}`,
			},
			wantO: &Object{
				Key:  "ABC",
				Size: 100,
			},
		},
		{
			name: "Test2",
			args: args{
				str: "Non-Json",
			},
			wantO: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotO := newObject(tt.args.str); !reflect.DeepEqual(gotO, tt.wantO) {
				t.Errorf("newObject() = %v, want %v", gotO, tt.wantO)
			}
		})
	}
}

func TestObjectToString(t *testing.T) {
	output := `{"Key":"ABC","Size":100}`
	type fields struct {
		Key  string
		Size int64
	}
	tests := []struct {
		name   string
		fields fields
		want   *string
	}{
		{
			name: "Test1",
			fields: fields{
				Key:  "ABC",
				Size: 100,
			},
			want: &output,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &Object{
				Key:  tt.fields.Key,
				Size: tt.fields.Size,
			}
			if got := o.toString(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Object.toString() = %v, want %v", got, tt.want)
			}
		})
	}
}
