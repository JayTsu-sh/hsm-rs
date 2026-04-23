/* bindgen entry point.
 * Pulls in the Lustre HSM ABI headers + liblustreapi userspace API.
 * The build script (build.rs) restricts emitted symbols to the HSM
 * subset to keep generated code small.
 */

#include <linux/lustre/lustre_user.h>
#include <linux/lustre/lustre_kernelcomm.h>
#include <linux/lustre/lustre_idl.h>
#include <lustre/lustreapi.h>
