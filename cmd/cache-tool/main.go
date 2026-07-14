package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/ankitm/mpv-relay/internal/config"
	"github.com/ankitm/mpv-relay/internal/db"
)

type OrphanFile struct {
	Name    string
	VideoID string
	Size    int64
}

type TableItem struct {
	Entry  db.CacheReportEntry
	Status string // "[EVICT]" or "[KEEP]"
}

func main() {
	sortOpt := flag.String("sort", "lru", "Sort order: 'lru' (oldest access first) or 'playcount' (least played first)")
	cleanOrphans := flag.Bool("clean-orphans", false, "Identify and list orphaned cache files (files on disk not in DB)")
	clearOpt := flag.Bool("clear", false, "Perform eviction of excess cache files (or delete orphans if -clean-orphans is set)")
	yesOpt := flag.Bool("yes", false, "Skip confirmation prompt when clearing")
	flag.Parse()

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	// Open database
	database, err := db.Open(cfg.DBPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database at %s: %v\n", cfg.DBPath, err)
		os.Exit(1)
	}
	defer database.Close()
	database.MediaDir = cfg.MediaDir

	// Migrate paths to relative if not already done
	if err := database.MigratePathsToRelative(); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: path migration failed: %v\n", err)
	}

	// Fetch cache report
	entries, err := database.GetCacheReport()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error fetching cache report: %v\n", err)
		os.Exit(1)
	}

	if *cleanOrphans {
		handleCleanOrphans(cfg, database, entries, *clearOpt, *yesOpt)
	} else {
		handleCacheList(cfg, database, entries, *sortOpt, *clearOpt, *yesOpt)
	}
}

func handleCacheList(cfg *config.Config, database *db.DB, entries []db.CacheReportEntry, sortOpt string, clearOpt, yesOpt bool) {
	// Sort entries
	if sortOpt == "playcount" {
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].PlayCount != entries[j].PlayCount {
				return entries[i].PlayCount < entries[j].PlayCount
			}
			return entries[i].LastAccessedAt.Before(entries[j].LastAccessedAt)
		})
	} else {
		// Default: LRU (oldest accessed first)
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].LastAccessedAt.Before(entries[j].LastAccessedAt)
		})
	}

	var totalSize int64
	for _, e := range entries {
		totalSize += e.FileSizeBytes
	}

	limitBytes := cfg.CacheMaxBytes
	excessBytes := totalSize - limitBytes

	fmt.Printf("Cache Directory: %s\n", cfg.MediaDir)
	fmt.Printf("Cache Limit:     %.2f MB (%d bytes)\n", float64(limitBytes)/1024/1024, limitBytes)
	fmt.Printf("Current Cache:   %.2f MB (%d bytes)\n", float64(totalSize)/1024/1024, totalSize)

	if excessBytes > 0 {
		fmt.Printf("Excess:          %.2f MB (%d bytes) -> OVER LIMIT\n\n", float64(excessBytes)/1024/1024, excessBytes)
	} else {
		fmt.Printf("Excess:          0.00 MB (0 bytes) -> WITHIN LIMIT\n\n")
	}

	var sumEvicted int64
	var items []TableItem
	for _, e := range entries {
		status := "[KEEP]"
		if excessBytes > 0 && sumEvicted < excessBytes {
			status = "[EVICT]"
			sumEvicted += e.FileSizeBytes
		}
		items = append(items, TableItem{Entry: e, Status: status})
	}

	if len(items) == 0 {
		fmt.Println("No items currently cached.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "INDEX\tTITLE\tVIDEO ID\tSIZE (MB)\tPLAY COUNT\tLAST ACCESSED\tSTATUS")
	for i, item := range items {
		title := item.Entry.Title
		if len(title) > 30 {
			title = title[:27] + "..."
		}
		sizeMB := float64(item.Entry.FileSizeBytes) / 1024 / 1024
		lastAccessed := item.Entry.LastAccessedAt.Format("2006-01-02 15:04:05")
		fmt.Fprintf(w, "%d\t%s\t%s\t%.2f\t%d\t%s\t%s\n",
			i+1, title, item.Entry.VideoID, sizeMB, item.Entry.PlayCount, lastAccessed, item.Status)
	}
	w.Flush()

	if excessBytes > 0 && clearOpt {
		var toEvict []TableItem
		for _, item := range items {
			if item.Status == "[EVICT]" {
				toEvict = append(toEvict, item)
			}
		}

		if len(toEvict) == 0 {
			return
		}

		if !yesOpt {
			fmt.Printf("\nAre you sure you want to evict %d tracks (freeing %.2f MB)? [y/N]: ", len(toEvict), float64(sumEvicted)/1024/1024)
			reader := bufio.NewReader(os.Stdin)
			response, _ := reader.ReadString('\n')
			response = strings.ToLower(strings.TrimSpace(response))
			if response != "y" && response != "yes" {
				fmt.Println("Aborted.")
				return
			}
		}

		fmt.Println("\nStarting eviction...")
		var successfullyFreed int64
		var count int
		for _, item := range toEvict {
			fmt.Printf("Evicting: %s (%s)... ", item.Entry.Title, item.Entry.VideoID)
			// Delete files from disk
			deleted := database.DeleteCacheFiles(item.Entry.VideoID, item.Entry.FilePath)
			// Delete entry from DB
			err := database.DeleteCacheByVideoID(item.Entry.VideoID)
			if err != nil {
				fmt.Printf("FAIL (DB delete: %v)\n", err)
			} else {
				successfullyFreed += item.Entry.FileSizeBytes
				count++
				if deleted {
					fmt.Println("OK (deleted files from disk)")
				} else {
					fmt.Println("OK (no files on disk, cleared DB reference)")
				}
			}
		}
		fmt.Printf("\nSuccessfully evicted %d tracks, freeing %.2f MB.\n", count, float64(successfullyFreed)/1024/1024)
	} else if excessBytes > 0 {
		fmt.Println("\nTip: Run with --clear to automatically evict the [EVICT] tracks.")
	}
}

func handleCleanOrphans(cfg *config.Config, database *db.DB, entries []db.CacheReportEntry, clearOpt, yesOpt bool) {
	refVideoIDs := make(map[string]bool)
	for _, e := range entries {
		refVideoIDs[e.VideoID] = true
	}

	files, err := os.ReadDir(cfg.MediaDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading media directory: %v\n", err)
		os.Exit(1)
	}

	var orphans []OrphanFile
	var totalOrphanSize int64

	for _, f := range files {
		if f.IsDir() || strings.HasPrefix(f.Name(), ".") {
			continue
		}

		// Split by dot to get the base video ID (e.g. videoID.ext)
		parts := strings.Split(f.Name(), ".")
		videoID := parts[0]

		// If video ID is not in our database references, it's an orphan
		if !refVideoIDs[videoID] {
			info, err := f.Info()
			size := int64(0)
			if err == nil {
				size = info.Size()
			}
			orphans = append(orphans, OrphanFile{
				Name:    f.Name(),
				VideoID: videoID,
				Size:    size,
			})
			totalOrphanSize += size
		}
	}

	fmt.Printf("Scanning Directory: %s\n", cfg.MediaDir)
	fmt.Printf("Found %d orphaned/unreferenced cache files.\n\n", len(orphans))

	if len(orphans) == 0 {
		fmt.Println("No orphaned files found.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "INDEX\tFILE NAME\tVIDEO ID\tSIZE (MB)")
	for i, file := range orphans {
		sizeMB := float64(file.Size) / 1024 / 1024
		fmt.Fprintf(w, "%d\t%s\t%s\t%.2f\n", i+1, file.Name, file.VideoID, sizeMB)
	}
	w.Flush()

	fmt.Printf("\nTotal Orphaned Size: %.2f MB (%d bytes)\n", float64(totalOrphanSize)/1024/1024, totalOrphanSize)

	if clearOpt {
		if !yesOpt {
			fmt.Printf("\nAre you sure you want to delete all %d orphaned files? [y/N]: ", len(orphans))
			reader := bufio.NewReader(os.Stdin)
			response, _ := reader.ReadString('\n')
			response = strings.ToLower(strings.TrimSpace(response))
			if response != "y" && response != "yes" {
				fmt.Println("Aborted.")
				return
			}
		}

		fmt.Println("\nStarting deletion of orphaned files...")
		var successfullyFreed int64
		var count int
		for _, file := range orphans {
			path := filepath.Join(cfg.MediaDir, file.Name)
			fmt.Printf("Deleting: %s... ", file.Name)
			if err := os.Remove(path); err != nil {
				fmt.Printf("FAIL (%v)\n", err)
			} else {
				successfullyFreed += file.Size
				count++
				fmt.Println("OK")
			}
		}
		fmt.Printf("\nSuccessfully deleted %d orphaned files, freeing %.2f MB.\n", count, float64(successfullyFreed)/1024/1024)
	} else {
		fmt.Println("\nTip: Run with --clean-orphans --clear to delete all listed orphaned files.")
	}
}
