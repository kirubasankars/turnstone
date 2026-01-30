package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"turnstone/client"
)

// --- Configuration ---

var (
	addr        = flag.String("addr", "localhost:6379", "TurnstoneDB server address")
	db          = flag.String("db", "1", "Database index to use")
	home        = flag.String("home", ".", "Path to home directory containing certs/")
	numWorkers  = flag.Int("workers", 50, "Number of concurrent workers")
	numKeys     = flag.Int("devices", 10000, "Number of unique devices to simulate")
	totalOps    = flag.Int("ops", 5000, "Total iterations per worker")
	baseSeed    = flag.Int64("seed", 42, "Base random seed")
	maxApps     = flag.Int("max-apps", 150, "Maximum apps per device")
	paddingSize = flag.Int("padding", 0, "Extra padding bytes")
)

// --- Data Models ---

// Key: device.{DeviceUUID}
type DeviceMeta struct {
	DeviceID    string `json:"device_uuid"`
	DeviceName  string `json:"device_name"`
	DeviceType  string `json:"device_type"`
	UpdateCount uint64 `json:"update_count"` // Persistent Counter
	IsActive    bool   `json:"is_active"`
	Padding     string `json:"padding,omitempty"`
}

// Key: {DeviceUUID}.app.{AppUUID}
type Application struct {
	DeviceID        string `json:"device_uuid"`
	AppID           string `json:"app_uuid"`
	Name            string `json:"application"`
	InstalledStatus string `json:"installed_status"`
	Status          string `json:"status"`
	UpdateCount     uint64 `json:"update_count"` // Persistent Counter
	InstallTime     string `json:"installation_time"`
	SeqID           int    `json:"app_seq_id"`
}

// --- Metrics ---

type OpStats struct {
	TXCommitted int64
	Conflicts   int64
	Failures    int64
	AppsAdded   int64
	AppsUpdated int64
	AppsRemoved int64
}

// --- Main ---

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	fmt.Printf("--- Event Pipeline: Sharded R-M-W Generator (Batch Mode) ---\n")
	fmt.Printf("Server:       %s\n", *addr)
	fmt.Printf("Database:     %s\n", *db)
	fmt.Printf("Concurrency:  %d workers\n", *numWorkers)
	fmt.Printf("Device Pool:  %d devices\n", *numKeys)
	fmt.Printf("Strategy:     Key Partitioning + MGET/MSET/MDEL\n")
	fmt.Println("--------------------------------------------------")

	caPath := filepath.Join(*home, "certs", "ca.crt")
	certPath := filepath.Join(*home, "certs", "client.crt")
	keyPath := filepath.Join(*home, "certs", "client.key")

	opsPerWorker := *totalOps / *numWorkers
	padStr := ""
	if *paddingSize > 0 {
		padStr = string(make([]byte, *paddingSize))
	}

	// Calculate Sharding Ranges
	rangeSize := *numKeys / *numWorkers
	if rangeSize == 0 {
		rangeSize = 1
	}

	var wg sync.WaitGroup
	var stats OpStats

	start := time.Now()

	for i := 0; i < *numWorkers; i++ {
		wg.Add(1)
		workerID := i

		// Partition Logic
		startID := workerID*rangeSize + 1
		endID := startID + rangeSize - 1
		if workerID == *numWorkers-1 {
			endID = *numKeys // Ensure last worker covers remainder
		}

		go func(wID, sID, eID int) {
			defer wg.Done()
			s := runWorker(wID, opsPerWorker, sID, eID, caPath, certPath, keyPath, padStr)

			// Aggregate stats atomically
			atomic.AddInt64(&stats.TXCommitted, s.TXCommitted)
			atomic.AddInt64(&stats.Conflicts, s.Conflicts)
			atomic.AddInt64(&stats.Failures, s.Failures)
			atomic.AddInt64(&stats.AppsAdded, s.AppsAdded)
			atomic.AddInt64(&stats.AppsUpdated, s.AppsUpdated)
			atomic.AddInt64(&stats.AppsRemoved, s.AppsRemoved)
		}(workerID, startID, endID)
	}

	wg.Wait()
	duration := time.Since(start)

	throughput := float64(stats.TXCommitted) / duration.Seconds()

	fmt.Println("\n--- Performance Results ---")
	fmt.Printf("Duration:     %v\n", duration)
	fmt.Printf("Total TXs:    %d\n", stats.TXCommitted)
	fmt.Printf("Conflicts:    %d (Should be 0 due to Sharding)\n", stats.Conflicts)
	fmt.Printf("Errors:       %d\n", stats.Failures)
	fmt.Printf("Throughput:   %.2f tx/sec\n", throughput)

	fmt.Println("\n--- Application Activity ---")
	fmt.Printf("Added:        %d\n", stats.AppsAdded)
	fmt.Printf("Updated:      %d (Read -> Inc -> Write)\n", stats.AppsUpdated)
	fmt.Printf("Removed:      %d\n", stats.AppsRemoved)
}

// --- Worker Loop ---

func runWorker(id int, iterations int, startID, endID int, ca, cert, key, padding string) OpStats {
	var stats OpStats

	// Initialize Client
	cl, err := client.NewMTLSClientHelper(*addr, ca, cert, key, nil)
	if err != nil {
		log.Printf("[Worker %d] Connect failed: %v", id, err)
		stats.Failures += int64(iterations * 2)
		return stats
	}
	defer cl.Close()

	// Select the database passed via command line
	if err := cl.Select(*db); err != nil {
		log.Printf("[Worker %d] Select DB %s failed: %v", id, *db, err)
		stats.Failures += int64(iterations * 2)
		return stats
	}

	// Deterministic RNG per worker
	rng := rand.New(rand.NewSource(*baseSeed + int64(id)))

	// Pre-calculate IDs for this worker's range to pick randomly from
	idRange := endID - startID + 1
	if idRange <= 0 {
		return stats
	}

	for i := 0; i < iterations; i++ {
		// 1. Select Random Device FROM ASSIGNED PARTITION
		deviceIdx := rng.Intn(idRange) + startID
		deviceUUID := GetDeviceUUID(deviceIdx)

		// 2. Define Keys
		deviceKey := fmt.Sprintf("device.%s", deviceUUID)
		appListKey := fmt.Sprintf("device.applications.%s", deviceUUID)

		// ---------------------------------------------------------
		// TRANSACTION 1: Update Device Metadata (Strict Get -> Set)
		// ---------------------------------------------------------
		for {
			if err := cl.Begin(); err != nil {
				stats.Failures++
				time.Sleep(50 * time.Millisecond)
				break
			}

			val, err := cl.Get(deviceKey)
			var meta DeviceMeta

			if err == client.ErrNotFound {
				// INSERT
				meta = GenerateDeviceMeta(deviceIdx, deviceUUID)
				meta.Padding = padding
				meta.UpdateCount = 1
			} else if err != nil {
				cl.Abort()
				stats.Failures++
				break
			} else {
				// UPDATE
				if json.Unmarshal(val, &meta) != nil {
					meta = GenerateDeviceMeta(deviceIdx, deviceUUID)
					meta.UpdateCount = 0
				}
				meta.UpdateCount++

				// Update Active Status (80/20)
				if rng.Float32() < 0.8 {
					meta.IsActive = true
				} else {
					meta.IsActive = false
				}
			}

			// Write
			bytes, _ := json.Marshal(meta)
			if err := cl.Set(deviceKey, bytes); err != nil {
				cl.Abort()
				stats.Failures++
				break
			}

			// Commit
			if err := cl.Commit(); err == nil {
				stats.TXCommitted++
				break
			} else if err == client.ErrTxConflict {
				stats.Conflicts++ // Should be rare/zero with sharding
				time.Sleep(time.Duration(rng.Intn(5)) * time.Millisecond)
				continue
			} else {
				stats.Failures++
				break
			}
		}

		// ---------------------------------------------------------
		// TRANSACTION 2: Sync Applications (Bulk R-M-W using MGET/MSET/MDEL)
		// ---------------------------------------------------------
		for {
			if err := cl.Begin(); err != nil {
				stats.Failures++
				break
			}

			// 1. Fetch Current List
			currentMap := make(map[string]struct{})
			val, err := cl.Get(appListKey)
			if err == nil {
				var storedList []string
				if json.Unmarshal(val, &storedList) == nil {
					for _, uid := range storedList {
						currentMap[uid] = struct{}{}
					}
				}
			} else if err != client.ErrNotFound {
				cl.Abort()
				stats.Failures++
				break
			}

			// 2. Generate Target
			targetApps := GenerateApplications(deviceIdx, deviceUUID, rng.Int63())

			// 3. Diff Logic
			var appsToUpdate []Application
			var appsToAdd []Application
			var appsToRemove []string
			var targetList []string

			for _, app := range targetApps {
				targetList = append(targetList, app.AppID)
				if _, exists := currentMap[app.AppID]; exists {
					appsToUpdate = append(appsToUpdate, app)
					delete(currentMap, app.AppID)
				} else {
					appsToAdd = append(appsToAdd, app)
				}
			}
			for appID := range currentMap {
				appsToRemove = append(appsToRemove, appID)
			}

			batchError := false

			// 4. Batch Read Updates (MGET)
			var updateKeys []string
			for _, app := range appsToUpdate {
				updateKeys = append(updateKeys, fmt.Sprintf("%s.app.%s", deviceUUID, app.AppID))
			}

			var oldVals [][]byte
			if len(updateKeys) > 0 {
				oldVals, err = cl.MGet(updateKeys...)
				if err != nil {
					batchError = true
				}
			}

			if batchError {
				cl.Abort()
				stats.Failures++
				break
			}

			// 5. Prepare Batch Writes (MSET)
			writeMap := make(map[string][]byte)

			// Process Updates
			for i, app := range appsToUpdate {
				oldVal := oldVals[i]
				if oldVal != nil {
					var oldApp Application
					if json.Unmarshal(oldVal, &oldApp) == nil {
						app.UpdateCount = oldApp.UpdateCount + 1
					} else {
						app.UpdateCount = 1
					}
				} else {
					// Should ideally be consistent if it was in the list, but handle missing key gracefully
					app.UpdateCount = 1
				}

				appBytes, _ := json.Marshal(app)
				writeMap[updateKeys[i]] = appBytes
				stats.AppsUpdated++
			}

			// Process Adds
			for i := range appsToAdd {
				app := &appsToAdd[i]
				appKey := fmt.Sprintf("%s.app.%s", deviceUUID, app.AppID)
				app.UpdateCount = 1

				appBytes, _ := json.Marshal(app)
				writeMap[appKey] = appBytes
				stats.AppsAdded++
			}

			// Process List Update
			listBytes, _ := json.Marshal(targetList)
			writeMap[appListKey] = listBytes

			if len(writeMap) > 0 {
				if err := cl.MSet(writeMap); err != nil {
					cl.Abort()
					stats.Failures++
					break
				}
			}

			// 6. Batch Removes (MDEL)
			var delKeys []string
			for _, appID := range appsToRemove {
				delKeys = append(delKeys, fmt.Sprintf("%s.app.%s", deviceUUID, appID))
				stats.AppsRemoved++
			}

			if len(delKeys) > 0 {
				if _, err := cl.MDel(delKeys...); err != nil {
					cl.Abort()
					stats.Failures++
					break
				}
			}

			// 7. Commit
			if err := cl.Commit(); err == nil {
				stats.TXCommitted++
				break
			} else if err == client.ErrTxConflict {
				stats.Conflicts++
				time.Sleep(time.Duration(rng.Intn(10)) * time.Millisecond)
				continue
			} else {
				stats.Failures++
				break
			}
		}
	}

	return stats
}

// --- Deterministic Generators ---

func GetDeviceUUID(id int) string {
	seed := int64(id)*1664525 + 1013904223
	r := rand.New(rand.NewSource(seed))

	uuidBytes := make([]byte, 16)
	r.Read(uuidBytes)

	return fmt.Sprintf("%x-%x-%x-%x-%x",
		uuidBytes[0:4], uuidBytes[4:6], uuidBytes[6:8], uuidBytes[8:10], uuidBytes[10:])
}

func GenerateDeviceMeta(id int, uuid string) DeviceMeta {
	seed := int64(id)*1664525 + 1013904223
	r := rand.New(rand.NewSource(seed))

	d := DeviceMeta{
		DeviceID:   uuid,
		DeviceName: fmt.Sprintf("device-%d", id),
	}

	types := []string{"android", "apple", "samsung", "appletv", "chromebook"}
	d.DeviceType = types[r.Intn(5)]

	if r.Float32() < 0.8 {
		d.IsActive = true
	} else {
		d.IsActive = false
	}

	return d
}

func GenerateApplications(deviceID int, deviceUUID string, seed int64) []Application {
	r := rand.New(rand.NewSource(seed)) // Volatile RNG (Changes every iteration)

	// Generate 1 to maxApps
	count := r.Intn(*maxApps) + 1
	apps := make([]Application, count)

	for i := 0; i < count; i++ {
		app := Application{
			DeviceID: deviceUUID,
			SeqID:    i + 1,
		}

		// STABLE IDENTITY:
		idSeed := int64(deviceID)*1000000 + int64(i)
		idRng := rand.New(rand.NewSource(idSeed))

		uuidBytes := make([]byte, 16)
		idRng.Read(uuidBytes)
		app.AppID = hex.EncodeToString(uuidBytes)

		app.Name = possibleApps[idRng.Intn(len(possibleApps))]

		// VOLATILE PROPERTIES:
		roll := r.Intn(10)
		switch {
		case roll == 0:
			app.InstalledStatus = "Pending Install"
		case roll == 1:
			app.InstalledStatus = "Update Required"
		case roll == 2:
			app.InstalledStatus = "Failed"
		default:
			app.InstalledStatus = "Installed"
		}

		if r.Intn(10) < 8 {
			app.Status = "Managed"
		} else {
			app.Status = "Unmanaged"
		}

		daysAgo := r.Intn(365)
		hoursAgo := r.Intn(24)
		ts := time.Now().AddDate(0, 0, -daysAgo).Add(time.Duration(-hoursAgo) * time.Hour)
		app.InstallTime = ts.Format(time.RFC3339)

		apps[i] = app
	}
	return apps
}

var possibleApps = []string{
	"1Password", "Adobe Acrobat Reader", "Adobe Illustrator", "Adobe Lightroom", "Adobe Photoshop",
	"Adobe Premiere Pro", "Adobe XD", "Airbnb", "Alfred", "Android Studio",
	"AnyDesk", "Apache NetBeans", "Arc Browser", "Asana", "Atom",
	"Audacity", "Autodesk AutoCAD", "Autodesk Maya", "AWS CLI", "Bitbucket",
	"Bitwarden", "Blender", "Box", "Brave Browser", "Calculator",
	"Calendar", "Canva", "Charles Proxy", "Cisco AnyConnect", "Cisco Webex",
	"Citrix Workspace", "ClickUp", "Clock", "Confluence", "Contacts",
	"CrowdStrike Falcon", "CyberDuck", "DaVinci Resolve", "Discord", "Docker Desktop",
	"Docusign", "Dropbox", "Eclipse IDE", "Emacs", "Evernote",
	"Figma", "FileZilla", "Final Cut Pro", "Firefox", "GarageBand",
	"GIMP", "Git", "GitHub Desktop", "GitKraken", "GitLab",
	"Gmail", "GoToMeeting", "Google Chrome", "Google Drive", "Google Earth",
	"Google Keep", "Google Maps", "Google Photos", "Grammarly", "HandBrake",
	"HashiCorp Vault", "HipChat", "Hyper", "iMovie", "Inkscape",
	"Insomnia", "Instagram", "IntelliJ IDEA", "iTerm2", "iTunes",
	"Jamf Pro", "Jenkins", "Jira", "JumpCloud", "Kafka Tool",
	"KeePass", "Keynote", "Kindle", "LastPass", "LibreOffice",
	"Linear", "LinkedIn", "Logic Pro", "Logitech Options", "Loom",
	"Lucidchart", "Magnet", "Mail", "Malwarebytes", "Mattermost",
	"McAfee Endpoint Security", "Messages", "Messenger", "Microsoft Access", "Microsoft Edge",
	"Microsoft Excel", "Microsoft OneNote", "Microsoft Outlook", "Microsoft PowerPoint", "Microsoft Teams",
	"Microsoft To Do", "Microsoft Word", "Miro", "Monday.com", "MongoDB Compass",
	"Mozilla Thunderbird", "MySQL Workbench", "Netflix", "NordVPN", "Notepad++",
	"Notion", "OBS Studio", "Obsidian", "Okta Verify", "OneDrive",
	"OpenVPN", "Opera", "Oracle VM VirtualBox", "Pages", "Parallels Desktop",
	"PgAdmin", "Photos", "Postman", "ProtonVPN", "PuTTY",
	"PyCharm", "Python", "QuickTime Player", "Raycast", "Rider",
	"Safari", "Salesforce", "SAP GUI", "SentinelOne", "ServiceNow",
	"Signal", "Sketch", "Skype", "Slack", "Snapchat",
	"Sophos Endpoint", "SourceTree", "Splunk", "Spotify", "SQL Server Management Studio",
}
