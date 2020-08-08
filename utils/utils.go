package utils

import "errors"

// ValidateFlags validates if are flags exist
func ValidateFlags(args ...string) error {
	for _, arg := range args {
		if arg == "" {
			return errors.New("Missing flag")
		}
	}
	return nil
}
