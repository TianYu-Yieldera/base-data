package middleware

import (
	"github.com/gin-gonic/gin"
)

// OptionalAuth returns a gin middleware for optional JWT authentication
// This middleware does not require authentication but extracts user info if present
func OptionalAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get authorization header
		authHeader := c.GetHeader("Authorization")

		if authHeader == "" {
			// No auth header, continue without user context
			c.Next()
			return
		}

		// TODO: Implement JWT validation when needed
		// For now, just continue
		c.Next()
	}
}

// RequireAuth returns a gin middleware that requires JWT authentication
func RequireAuth(jwtSecret string) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")

		if authHeader == "" {
			c.AbortWithStatusJSON(401, gin.H{
				"code":    "UNAUTHORIZED",
				"message": "Authorization header required",
			})
			return
		}

		// TODO: Implement JWT validation
		// For now, just continue
		c.Next()
	}
}
