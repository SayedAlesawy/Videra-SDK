package viderasdk

// VideraSDK Handles communication between clients and videra system
type VideraSDK struct {
	masterURL          string //IP of master
	chunkSize          int64  //Upload chunk size
	defaultMaxRetries  int    //Max number of request retrials
	defaultWaitingTime int    //waiting time between failed request and new one
}
