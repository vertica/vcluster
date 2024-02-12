package vclusterops

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShowRestorePointFilterOptions_ValidateAndStandardizeTimestampsIfAny(t *testing.T) {
	// Test case 1: No validation needed
	filterOptions := ShowRestorePointFilterOptions{
		StartTimestamp: nil,
		EndTimestamp:   nil,
	}
	err := filterOptions.ValidateAndStandardizeTimestampsIfAny()
	assert.NoError(t, err)

	// Test case 2: Invalid start timestamp
	startTimestamp := "invalid_start_timestamp"
	filterOptions = ShowRestorePointFilterOptions{
		StartTimestamp: &startTimestamp,
		EndTimestamp:   nil,
	}
	err = filterOptions.ValidateAndStandardizeTimestampsIfAny()
	expectedErr := fmt.Errorf("start timestamp %q is invalid;", startTimestamp)
	assert.ErrorContains(t, err, expectedErr.Error())

	// Test case 3: Invalid end timestamp
	endTimestamp := "invalid_end_timestamp"
	filterOptions = ShowRestorePointFilterOptions{
		StartTimestamp: nil,
		EndTimestamp:   &endTimestamp,
	}
	err = filterOptions.ValidateAndStandardizeTimestampsIfAny()
	expectedErr = fmt.Errorf("end timestamp %q is invalid;", endTimestamp)
	assert.ErrorContains(t, err, expectedErr.Error())

	const earlierDate = "2022-01-01"
	const laterDate = "2022-01-02"

	// Test case 4: Valid start and end timestamps
	startTimestamp = earlierDate + " 00:00:00"
	endTimestamp = laterDate + " 00:00:00"
	filterOptions = ShowRestorePointFilterOptions{
		StartTimestamp: &startTimestamp,
		EndTimestamp:   &endTimestamp,
	}
	err = filterOptions.ValidateAndStandardizeTimestampsIfAny()
	assert.NoError(t, err)

	startTimestamp = earlierDate
	endTimestamp = laterDate
	err = filterOptions.ValidateAndStandardizeTimestampsIfAny()
	assert.NoError(t, err)
	assert.Equal(t, earlierDate+" 00:00:00.000000000", *filterOptions.StartTimestamp)
	assert.Equal(t, laterDate+" 23:59:59.999999999", *filterOptions.EndTimestamp)

	startTimestamp = earlierDate
	endTimestamp = earlierDate
	err = filterOptions.ValidateAndStandardizeTimestampsIfAny()
	assert.NoError(t, err)

	startTimestamp = earlierDate
	endTimestamp = laterDate + " 23:59:59"
	err = filterOptions.ValidateAndStandardizeTimestampsIfAny()
	assert.NoError(t, err)

	startTimestamp = earlierDate + " 01:01:01.010101010"
	endTimestamp = laterDate
	err = filterOptions.ValidateAndStandardizeTimestampsIfAny()
	assert.NoError(t, err)
	assert.Equal(t, startTimestamp, *filterOptions.StartTimestamp)

	startTimestamp = earlierDate + " 23:59:59"
	endTimestamp = earlierDate + " 23:59:59.123456789"
	err = filterOptions.ValidateAndStandardizeTimestampsIfAny()
	assert.NoError(t, err)

	// Test case 5: Start timestamp after end timestamp
	filterOptions = ShowRestorePointFilterOptions{
		StartTimestamp: &endTimestamp,
		EndTimestamp:   &startTimestamp,
	}
	err = filterOptions.ValidateAndStandardizeTimestampsIfAny()
	assert.EqualError(t, err, "start timestamp must be before end timestamp")
}
