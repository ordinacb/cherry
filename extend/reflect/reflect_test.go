package cherryReflect

import "testing"

type FunStruct struct {
}

func (f *FunStruct) A() string {
	return "A method"
}

func test1111() {}

func TestGetFuncName(t *testing.T) {
	f := &FunStruct{}

	if got := GetStructName(f); got != "FunStruct" {
		t.Fatalf("struct name: %s", got)
	}
	if got := GetFuncName(f.A); got != "A" {
		t.Fatalf("method name: %s", got)
	}
	if got := GetFuncName(test1111); got != "test1111" {
		t.Fatalf("function name: %s", got)
	}
	t.Run("reject_non_function", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("non-function was accepted")
			}
		}()
		GetFuncName(f)
	})
}

func TestIsPtr(t *testing.T) {
	if IsPtr(nil) || !IsPtr(&FunStruct{}) {
		t.Fatal("incorrect pointer classification")
	}
}

func TestIsNotPtr(t *testing.T) {
	if IsNotPtr(&FunStruct{}) || !IsNotPtr(FunStruct{}) {
		t.Fatal("incorrect non-pointer classification")
	}
}
