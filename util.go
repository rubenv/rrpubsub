package rrpubsub

func sToI(s []string) []interface{} {
	r := make([]interface{}, 0, len(s))
	for _, str := range s {
		r = append(r, str)
	}
	return r
}
